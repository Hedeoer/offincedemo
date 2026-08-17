package cn.hedeoer.udaf;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import java.util.Arrays;

/**
 * 事件转化率聚合函数。
 *
 * <p>使用场景：基于一张用户行为事件明细表，统计某类事件相对于另一类事件的发生比例。
 * 例如在同一个日期、渠道或商品维度下，统计支付事件数相对于浏览事件数的比例。</p>
 *
 * <p>模拟单表结构如下：</p>
 *
 * <pre>
 * dwd_user_event_log (
 *     dt          string,  -- 事件日期
 *     user_id     string,  -- 用户 ID
 *     session_id  string,  -- 会话 ID
 *     event_time  string,  -- 事件时间
 *     channel     string,  -- 访问渠道，例如 app、web
 *     event_type  string,  -- 事件类型，例如 view、click、pay
 *     product_id  string,  -- 商品 ID
 *     amount      int,     -- 支付金额，非支付事件可为 0
 *     tags        string,  -- 用户或事件标签
 *     item_list   string   -- 商品明细字符串，后续可用于 UDTF 学习
 * )
 * </pre>
 *
 * <p>函数签名：</p>
 *
 * <pre>
 * event_rate(event_type, numerator_event, denominator_event)
 * </pre>
 *
 * <p>示例 SQL：</p>
 *
 * <pre>
 * SELECT
 *     dt,
 *     channel,
 *     event_rate(event_type, 'pay', 'view') AS pay_view_rate
 * FROM dwd_user_event_log
 * GROUP BY dt, channel;
 * </pre>
 *
 * <p>计算逻辑：在每个分组内统计 event_type = 'pay' 的行数作为分子，
 * 统计 event_type = 'view' 的行数作为分母，最终返回 pay_count / view_count。
 * 如果分母计数为 0，则返回 null。</p>
 *
 * <p>例如 app 渠道内有 1 次 pay、3 次 view，则返回 0.3333333333；
 * web 渠道内有 1 次 pay、2 次 view，则返回 0.5。</p>
 *
 * <p>兼容性说明：Spark 3.3.1 识别 Hive UDAF 时，需要函数入口类继承
 * AbstractGenericUDAFResolver，再由 Resolver 返回具体的 GenericUDAFEvaluator。
 * 如果入口类直接继承 GenericUDAFEvaluator 并实现 GenericUDAFResolver2，
 * Hive 本地测试可能通过，但 Spark SQL 中可能报 No handler for UDF/UDAF/UDTF。</p>
 */
public class EventRateUDAF extends AbstractGenericUDAFResolver {

    /**
     * 校验 UDAF 的参数个数和参数类型，并返回实际执行聚合逻辑的 Evaluator。
     *
     * @param parameters SQL 中传入的参数类型数组
     * @return 用于实际执行聚合逻辑的 UDAF 计算器实例
     * @throws SemanticException 参数个数不是 3 个或参数不是字符串类型时抛出
     */
    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {
        if (parameters.length != 3) {
            throw new SemanticException("event_rate requires three arguments: event_type, numerator_event, denominator_event");
        }

        for (int i = 0; i < parameters.length; i++) {
            if (parameters[i].getCategory() != ObjectInspector.Category.PRIMITIVE) {
                throw new SemanticException("event_rate argument " + (i + 1) + " must be a primitive string type");
            }

            PrimitiveObjectInspector.PrimitiveCategory primitiveCategory =
                    ((PrimitiveTypeInfo) parameters[i]).getPrimitiveCategory();
            if (primitiveCategory != PrimitiveObjectInspector.PrimitiveCategory.STRING
                    && primitiveCategory != PrimitiveObjectInspector.PrimitiveCategory.CHAR
                    && primitiveCategory != PrimitiveObjectInspector.PrimitiveCategory.VARCHAR) {
                throw new SemanticException("event_rate argument " + (i + 1) + " must be string, char, or varchar");
            }
        }

        return new EventRateEvaluator();
    }

    /**
     * 事件转化率 UDAF 的实际聚合计算器。
     *
     * <p>Resolver 负责参数校验和选择 Evaluator；Evaluator 负责 UDAF 生命周期中的
     * init、iterate、terminatePartial、merge、terminate 等实际聚合逻辑。</p>
     */
    public static class EventRateEvaluator extends GenericUDAFEvaluator {

        /**
         * 原始输入阶段使用的 event_type 参数解析器。
         *
         * <p>在 PARTIAL1 或 COMPLETE 阶段的 init 方法中初始化，
         * 在 iterate 方法中用于把当前行的事件类型读取成 Java 字符串。</p>
         */
        private PrimitiveObjectInspector eventTypeOI;

        /**
         * 原始输入阶段使用的分子事件参数解析器。
         *
         * <p>在 PARTIAL1 或 COMPLETE 阶段的 init 方法中初始化，
         * 在 iterate 方法中用于读取 numerator_event，例如 SQL 中传入的 'pay'。</p>
         */
        private PrimitiveObjectInspector numeratorEventOI;

        /**
         * 原始输入阶段使用的分母事件参数解析器。
         *
         * <p>在 PARTIAL1 或 COMPLETE 阶段的 init 方法中初始化，
         * 在 iterate 方法中用于读取 denominator_event，例如 SQL 中传入的 'view'。</p>
         */
        private PrimitiveObjectInspector denominatorEventOI;

        /**
         * 中间结果阶段使用的 struct 解析器。
         *
         * <p>在 PARTIAL2 或 FINAL 阶段的 init 方法中初始化，
         * 在 merge 方法中用于读取 terminatePartial 输出的中间结果结构。</p>
         */
        private StructObjectInspector partialOI;

        /**
         * 中间结果中的分子计数字段。
         *
         * <p>字段名对应 terminatePartial 返回结构中的 numerator_count，
         * 在 merge 方法中用于定位并读取分子事件累计次数。</p>
         */
        private StructField numeratorCountField;

        /**
         * 中间结果中的分母计数字段。
         *
         * <p>字段名对应 terminatePartial 返回结构中的 denominator_count，
         * 在 merge 方法中用于定位并读取分母事件累计次数。</p>
         */
        private StructField denominatorCountField;

        /**
         * 中间结果中 numerator_count 字段的基础类型解析器。
         *
         * <p>在 merge 方法中配合 PrimitiveObjectInspectorUtils.getLong 使用，
         * 负责把中间结果里的分子计数转换为 long。</p>
         */
        private PrimitiveObjectInspector partialNumeratorCountOI;

        /**
         * 中间结果中 denominator_count 字段的基础类型解析器。
         *
         * <p>在 merge 方法中配合 PrimitiveObjectInspectorUtils.getLong 使用，
         * 负责把中间结果里的分母计数转换为 long。</p>
         */
        private PrimitiveObjectInspector partialDenominatorCountOI;

        /**
         * terminatePartial 方法复用的中间结果数组。
         *
         * <p>数组下标 0 保存 numerator_count，下标 1 保存 denominator_count。
         * 复用该数组可以避免每次输出局部聚合结果时都创建新数组。</p>
         */
        private final Object[] partialResult = new Object[2];

        /**
         * 当前分组的聚合缓冲区。
         *
         * <p>Hive 会为不同分组创建不同的 AggregationBuffer，UDAF 的中间状态必须保存在这里，
         * 不能保存在 Evaluator 的普通成员变量中。</p>
         */
        static class EventRateBuffer extends AbstractAggregationBuffer {
            /**
             * 当前聚合缓冲区中的分子事件计数。
             *
             * <p>iterate 方法在 event_type 等于 numerator_event 时递增，
             * merge 方法在合并中间结果时累加。</p>
             */
            long numeratorCount;

            /**
             * 当前聚合缓冲区中的分母事件计数。
             *
             * <p>iterate 方法在 event_type 等于 denominator_event 时递增，
             * merge 方法在合并中间结果时累加。terminate 方法会用它判断是否可以计算比例。</p>
             */
            long denominatorCount;
        }

        /**
         * 初始化不同聚合阶段需要的 ObjectInspector，并声明当前阶段的返回类型。
         *
         * <p>Hive UDAF 为了支持分布式聚合，会把一次聚合拆成多个阶段：</p>
         *
         * <p>PARTIAL1：从原始输入行生成局部聚合结果。这个阶段会调用 iterate 处理原始数据，
         * 再调用 terminatePartial 输出中间结果。例如：event_type、'pay'、'view' -> struct<分子计数, 分母计数>。</p>
         *
         * <p>PARTIAL2：从其他 PARTIAL 阶段产生的中间结果继续合并成新的中间结果。这个阶段会调用 merge，
         * 再调用 terminatePartial 输出更大的局部聚合结果。例如：多个 struct<分子计数, 分母计数> -> 一个新的 struct<分子计数, 分母计数>。</p>
         *
         * <p>FINAL：从 PARTIAL 阶段产生的中间结果生成最终结果。这个阶段会调用 merge，
         * 再调用 terminate 输出最终值。例如：struct<分子计数, 分母计数> -> double 比例。</p>
         *
         * <p>COMPLETE：不拆分阶段，直接从原始输入行生成最终结果。这个阶段会调用 iterate，
         * 再调用 terminate 输出最终值。例如：event_type、'pay'、'view' -> double 比例。</p>
         *
         * @param mode Hive 当前执行阶段，可能是 PARTIAL1、PARTIAL2、FINAL 或 COMPLETE
         * @param parameters 当前阶段的输入参数 ObjectInspector
         * @return 当前阶段输出值的 ObjectInspector
         * @throws HiveException 初始化 ObjectInspector 失败时抛出
         */
        @Override
        public ObjectInspector init(Mode mode, ObjectInspector[] parameters) throws HiveException {
            super.init(mode, parameters);

            if (mode == Mode.PARTIAL1 || mode == Mode.COMPLETE) {
                eventTypeOI = (PrimitiveObjectInspector) parameters[0];
                numeratorEventOI = (PrimitiveObjectInspector) parameters[1];
                denominatorEventOI = (PrimitiveObjectInspector) parameters[2];
            } else {
                partialOI = (StructObjectInspector) parameters[0];
                numeratorCountField = partialOI.getStructFieldRef("numerator_count");
                denominatorCountField = partialOI.getStructFieldRef("denominator_count");
                partialNumeratorCountOI = (PrimitiveObjectInspector) numeratorCountField.getFieldObjectInspector();
                partialDenominatorCountOI = (PrimitiveObjectInspector) denominatorCountField.getFieldObjectInspector();
            }

            if (mode == Mode.PARTIAL1 || mode == Mode.PARTIAL2) {
                return ObjectInspectorFactory.getStandardStructObjectInspector(
                        Arrays.asList("numerator_count", "denominator_count"),
                        Arrays.asList(
                                PrimitiveObjectInspectorFactory.javaLongObjectInspector,
                                PrimitiveObjectInspectorFactory.javaLongObjectInspector
                        )
                );
            }

            return PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;
        }

        /**
         * 创建新的聚合缓冲区，用于保存当前分组内的中间计数状态。
         *
         * @return 初始化后的聚合缓冲区
         * @throws HiveException 创建或重置缓冲区失败时抛出
         */
        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            EventRateBuffer buffer = new EventRateBuffer();
            reset(buffer);
            return buffer;
        }

        /**
         * 重置聚合缓冲区，将分子事件计数和分母事件计数清零。
         *
         * @param agg 当前分组对应的聚合缓冲区
         * @throws HiveException 重置缓冲区失败时抛出
         */
        @Override
        public void reset(AggregationBuffer agg) throws HiveException {
            EventRateBuffer buffer = (EventRateBuffer) agg;
            buffer.numeratorCount = 0L;
            buffer.denominatorCount = 0L;
        }

        /**
         * 处理原始输入行，并根据事件类型更新聚合缓冲区中的计数。
         *
         * @param agg 当前分组对应的聚合缓冲区
         * @param parameters 原始输入参数，依次为 event_type、numerator_event、denominator_event
         * @throws HiveException 读取参数或更新缓冲区失败时抛出
         */
        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
            if (parameters == null || parameters.length != 3
                    || parameters[0] == null || parameters[1] == null || parameters[2] == null) {
                return;
            }

            EventRateBuffer buffer = (EventRateBuffer) agg;
            String eventType = PrimitiveObjectInspectorUtils.getString(parameters[0], eventTypeOI);
            String numeratorEvent = PrimitiveObjectInspectorUtils.getString(parameters[1], numeratorEventOI);
            String denominatorEvent = PrimitiveObjectInspectorUtils.getString(parameters[2], denominatorEventOI);

            if (eventType == null || numeratorEvent == null || denominatorEvent == null) {
                return;
            }

            if (eventType.equals(numeratorEvent)) {
                buffer.numeratorCount++;
            }

            if (eventType.equals(denominatorEvent)) {
                buffer.denominatorCount++;
            }
        }

        /**
         * 生成局部聚合结果，供 PARTIAL2 或 FINAL 阶段继续合并。
         *
         * @param agg 当前分组对应的聚合缓冲区
         * @return 包含分子计数和分母计数的中间结果
         * @throws HiveException 生成局部聚合结果失败时抛出
         */
        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            EventRateBuffer buffer = (EventRateBuffer) agg;
            partialResult[0] = buffer.numeratorCount;
            partialResult[1] = buffer.denominatorCount;
            return partialResult;
        }

        /**
         * 合并其他任务或阶段产生的局部聚合结果。
         *
         * @param agg 当前分组对应的聚合缓冲区
         * @param partial 其他阶段传入的局部聚合结果
         * @throws HiveException 读取或合并局部聚合结果失败时抛出
         */
        @Override
        public void merge(AggregationBuffer agg, Object partial) throws HiveException {
            if (partial == null) {
                return;
            }

            EventRateBuffer buffer = (EventRateBuffer) agg;
            Object numeratorCount = partialOI.getStructFieldData(partial, numeratorCountField);
            Object denominatorCount = partialOI.getStructFieldData(partial, denominatorCountField);

            if (numeratorCount != null) {
                buffer.numeratorCount += PrimitiveObjectInspectorUtils.getLong(numeratorCount, partialNumeratorCountOI);
            }

            if (denominatorCount != null) {
                buffer.denominatorCount += PrimitiveObjectInspectorUtils.getLong(denominatorCount, partialDenominatorCountOI);
            }
        }

        /**
         * 生成最终聚合结果，返回分子事件数除以分母事件数的比例。
         *
         * @param agg 当前分组对应的聚合缓冲区
         * @return 分母计数为 0 时返回 null，否则返回 double 类型比例
         * @throws HiveException 生成最终结果失败时抛出
         */
        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            EventRateBuffer buffer = (EventRateBuffer) agg;
            if (buffer.denominatorCount == 0L) {
                return null;
            }

            return (double) buffer.numeratorCount / buffer.denominatorCount;
        }
    }
}

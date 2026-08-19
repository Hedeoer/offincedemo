package cn.hedeoer.udtf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;

import java.util.Arrays;

/**
 * 商品明细展开型 UDTF。
 *
 * <p>使用场景：基于一张用户行为或订单明细表中的 item_list 字段，把一行商品串拆成多行明细。
 * 例如 item_list = "sku01:1:2999,sku02:2:1599" 时，展开后得到两行：
 * sku01 / 1 / 2999 和 sku02 / 2 / 1599。</p>
 *
 * <p>模拟单表中的相关字段：</p>
 *
 * <pre>
 * dwd_user_event_log (
 *     dt         string,
 *     channel    string,
 *     event_type string,
 *     item_list  string   -- 商品明细，格式为 sku_id:qty:price,sku_id:qty:price
 * )
 * </pre>
 *
 * <p>函数签名：</p>
 *
 * <pre>
 * item_list_explode(item_list)
 * </pre>
 *
 * <p>示例 SQL：</p>
 *
 * <pre>
 * CREATE TEMPORARY FUNCTION item_list_explode
 * AS 'cn.hedeoer.udtf.ItemListExplodeUDTF';
 *
 * SELECT
 *     dt,
 *     channel,
 *     t.sku_id,
 *     t.qty,
 *     t.price
 * FROM dwd_user_event_log
 * LATERAL VIEW item_list_explode(item_list) t AS sku_id, qty, price;
 * </pre>
 *
 * <p>该 UDTF 的核心作用是演示“一行输入展开为多行输出”的处理方式。
 * 当输入为空或 null 时不输出任何行；当某个商品片段格式不合法时，直接抛出异常，便于发现脏数据。</p>
 */
@Description(
        name = "item_list_explode",
        value = "_FUNC_(item_list) - Expands an item list string into multiple item detail rows",
        extended = "Arguments:\n" +
                "  item_list: String in sku_id:qty:price,sku_id:qty:price format.\n" +
                "Output columns:\n" +
                "  sku_id string, qty int, price int\n" +
                "Example:\n" +
                "  SELECT ... FROM t LATERAL VIEW item_list_explode(item_list) x AS sku_id, qty, price"
)
public class ItemListExplodeUDTF extends GenericUDTF {

    /**
     * 输入参数解析器，用于把 item_list 读取成 Java 字符串。
     *
     * <p>在 initialize 阶段完成初始化，在 process 阶段用于解析原始输入值。</p>
     */
    private PrimitiveObjectInspector inputOI;

    /**
     * 初始化 UDTF 的输入校验和输出结构。
     *
     * <p>UDTF 的输入必须只有 1 列，并且该列必须是字符串类型或兼容字符串的原始类型。</p>
     *
     * @param arguments 输入参数的 ObjectInspector 数组
     * @return UDTF 输出行的结构描述
     * @throws UDFArgumentException 参数个数或参数类型不符合要求时抛出
     */
    @Override
    public StructObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments == null || arguments.length != 1) {
            throw new UDFArgumentLengthException("item_list_explode requires exactly one argument");
        }

        if (!(arguments[0] instanceof PrimitiveObjectInspector)) {
            throw new UDFArgumentException("item_list_explode only accepts a primitive string argument");
        }

        inputOI = (PrimitiveObjectInspector) arguments[0];
        PrimitiveObjectInspector.PrimitiveCategory category = inputOI.getPrimitiveCategory();
        if (category != PrimitiveObjectInspector.PrimitiveCategory.STRING
                && category != PrimitiveObjectInspector.PrimitiveCategory.CHAR
                && category != PrimitiveObjectInspector.PrimitiveCategory.VARCHAR) {
            throw new UDFArgumentException("item_list_explode only accepts string, char, or varchar");
        }

        return ObjectInspectorFactory.getStandardStructObjectInspector(
                Arrays.asList("sku_id", "qty", "price"),
                Arrays.asList(
                        PrimitiveObjectInspectorFactory.javaStringObjectInspector,
                        PrimitiveObjectInspectorFactory.javaIntObjectInspector,
                        PrimitiveObjectInspectorFactory.javaIntObjectInspector
                )
        );
    }

    /**
     * 处理一行输入，并把 item_list 拆成多行输出。
     *
     * <p>输入格式约定为：sku_id:qty:price,sku_id:qty:price。</p>
     *
     * @param record 当前输入行，只有一个元素 item_list
     * @throws HiveException 解析格式或向下游输出失败时抛出
     */
    @Override
    public void process(Object[] record) throws HiveException {
        if (record == null || record.length != 1 || record[0] == null) {
            return;
        }

        String itemList = PrimitiveObjectInspectorUtils.getString(record[0], inputOI);
        if (itemList == null || itemList.trim().isEmpty()) {
            return;
        }

        String[] items = itemList.split(",");
        for (String item : items) {
            String trimmedItem = item == null ? null : item.trim();
            if (trimmedItem == null || trimmedItem.isEmpty()) {
                continue;
            }

            String[] parts = trimmedItem.split(":");
            if (parts.length != 3) {
                throw new HiveException("Invalid item_list fragment: " + trimmedItem);
            }

            Object[] row = new Object[3];
            row[0] = parts[0].trim();
            row[1] = parseInt(parts[1].trim(), "qty", trimmedItem);
            row[2] = parseInt(parts[2].trim(), "price", trimmedItem);
            forward(row);
        }
    }

    /**
     * 关闭 UDTF，当前实现无需释放额外资源。
     *
     * @throws HiveException 关闭过程中出现异常时抛出
     */
    @Override
    public void close() throws HiveException {
        // 当前没有额外资源需要关闭
    }

    /**
     * 将字符串解析成整数，并在格式非法时抛出异常。
     *
     * @param value 待解析的字符串
     * @param fieldName 字段名，用于错误提示
     * @param source 原始商品片段，用于错误提示
     * @return 解析后的整数
     * @throws HiveException 字段格式不合法时抛出
     */
    private Integer parseInt(String value, String fieldName, String source) throws HiveException {
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw new HiveException("Invalid " + fieldName + " value in item_list fragment: " + source, e);
        }
    }
}

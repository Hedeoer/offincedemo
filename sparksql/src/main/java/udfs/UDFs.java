package udfs;

import com.oracle.jrockit.jfr.DataType;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataType$;
import org.apache.spark.sql.types.DataTypes;
import org.junit.Test;

import static org.apache.spark.sql.functions.udf;

public class UDFs {

    @Test
    public void testUDF() throws AnalysisException {

        JavaSparkContext sc = new JavaSparkContext(new SparkContext("local[*]", "UDFs"));

        SparkSession spark = SparkSession.builder()
                .appName("UDFs")
                .master("local[*]")
                .getOrCreate();

        // 定义一个0参的UDF
        UserDefinedFunction random = udf( () -> Math.random(), DataTypes.DoubleType);
        // 使得自定义函数非确定性 标记 UDF 为非确定性可以防止 Spark 对其结果进行缓存，因为缓存非确定性函数的结果可能导致不一致的数据
        random.asNondeterministic();
        // 然 Spark 自动为 UDF 生成名称，但在某些情况下，指定一个有意义的名称可以使 SQL 查询或错误消息更具可读性和诊断性。此外，当 UDF 被注册到 Spark SQL 的 catalog 中时，指定的名称可以用于引用该函数。
        random.withName("random");
        // 注册使用
        spark.udf().register("random", random);

        spark.sql("select random()").show();


        // 定义一个1参的UDF
        UserDefinedFunction random1 = udf( (Integer i) -> 1 +  i, DataTypes.IntegerType);
        random1.asNonNullable();

        spark.udf().register("random1", random1);
        spark.sql("select random1(1)").show();

        // 在where 子句中使用UDF
        spark.udf().register("filterGt",
                udf((Long x ) -> x > 100,DataTypes.BooleanType));

        spark.range(1000).createTempView("range");
        spark.sql("select * from range where filterGt(id)").show();

    }
}

package datasources;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

/**
 * 读取mysql实践
 */
public class ReadMysql {
    public static void main(String[] args) throws AnalysisException {


        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .master("local[*]")
                .getOrCreate();



        // 表名
        String tableName = "gmall.order_detail";
        // url
        String url = "jdbc:mysql://hadoop102:3306/gmall";
        // 列名,类型必须是   numeric, date, or timestamp
        String columnName = "id";
        // 列名对应的最小值,用于后续RDD内部分区时的步长
        Long lowerBound = 0L;
        // 列名对应的最大值，用于后续RDD内部分区时的步长
        Long upperBound = 100000L;
        Integer defaultPartitionNums = 4;

        Properties properties = new Properties();
        properties.put("user","root");
        properties.put("password","aaaaaa");
//        properties.put("fetchsize","50000000"); // 50MB
//        properties.put("queryTimeout","1800000");// 30mins超时时间
        properties.put("driver","com.mysql.jdbc.Driver");


//        Dataset<Row> source1 = spark.read().jdbc(url, tableName, properties);
        // 获取分区数 默认为 1
//        System.out.println(source1.rdd().getNumPartitions());


        // 发出预请求，获取记录数最小值和最大值（通常是主键）
        Dataset<Row> prefix = spark.read().jdbc(url, "(select min(id) min_id , max(id) max_id from order_detail) order_details", properties);
        Row summaryMesTable = prefix.first();

        Long minId = (Long)summaryMesTable.getAs("min_id");
        Long maxId = (Long)summaryMesTable.getAs("max_id");
        System.out.println(minId + "==>" + maxId);

        Dataset<Row> finalSource = spark.read().jdbc(url, tableName, columnName, minId, maxId, defaultPartitionNums, properties);
        // 4
        System.out.println(finalSource.rdd().getNumPartitions());

        finalSource.createTempView("order_detail");
        // 统计每天的订单总额
        spark.sql("select\n" +
                "    dt,\n" +
                "    sum(order_price) price_dt\n" +
                "    from (\n" +
                "select\n" +
                "    substr(create_time,1, 10) dt,\n" +
                "    order_id,\n" +
                "    sum(sku_num * order_price) order_price\n" +
                "from order_detail t1\n" +
                "group by substr(create_time,1,10), order_id\n" +
                "    ) t2\n" +
                "group by dt;").show();
//+----------+----------+
//|        dt|  price_dt|
//+----------+----------+
//|2022-06-07|1224429.00|
//|2022-06-04|1300090.00|
//|2022-06-11|3559058.00|
//|2022-06-05|1235860.00|
//|2022-06-08|1557206.00|
//|2022-06-06|1425931.00|
//|2022-06-09|1403204.00|
//|2022-06-12|1551996.00|
//|2022-06-10|1460596.00|
//+----------+----------+

    }
}

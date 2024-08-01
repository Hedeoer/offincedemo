package datasources;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;

public class FileSource {

    /**
     * Json数据源
     * 直接使用sql语句 + 文件地址读取 ！！！
     */
    @Test
    public void JsonSource(){

        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .master("local[*]")
                .getOrCreate();
        Dataset<Row> sqlDF =
                spark.sql("SELECT * FROM json.`data/company.json`");

        sqlDF.show();

/*
* +-------+------+
|   name|salary|
+-------+------+
|Michael|  3000|
|   Andy|  4500|
| Justin|  3500|
|  Berta|  4000|
+-------+------+
* */
    }

    @Test
    public void  FileSourcePartition(){
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .master("local[*]")
                .getOrCreate();
        // 读取json数据
        Dataset<Row> sqlDF =
                spark.sql("SELECT * FROM json.`data/company.json`");


        // 写出成分区表
        sqlDF
                .write()
                .partitionBy("squadName")
                .saveAsTable("people_partitioned");

        // 从本地读取分区表的数据
        Dataset<Row> localDF =
                spark.sql("SELECT * FROM parquet.`spark-warehouse/people_partitioned`");

        localDF.show();
/*
+------+------+----------+--------------------+---------------+----------------+
|active|formed|  homeTown|             members|     secretBase|       squadName|
+------+------+----------+--------------------+---------------+----------------+
| false|  2008|Dark Haven|[{42, Night Stalk...|Underworld Lair|Shadow Syndicate|
| false|  2008|Dark Haven|[{42, Night Stalk...|Underworld Lair|Shadow Syndicate|
| false|  2008|Dark Haven|[{42, Night Stalk...|Underworld Lair|Shadow Syndicate|
|  true|  2005|    Gotham|[{45, Phantom Mag...| Shadow Mansion|Mystic Guardians|
|  true|  2005|    Gotham|[{45, Phantom Mag...| Shadow Mansion|Mystic Guardians|
+------+------+----------+--------------------+---------------+----------------+

* */

    }


}

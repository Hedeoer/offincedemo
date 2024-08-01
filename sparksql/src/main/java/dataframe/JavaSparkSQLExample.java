package dataframe;


import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class JavaSparkSQLExample {

    /*
    * {
  "squadName": "Super hero squad",
  "homeTown": "Metro City",
  "formed": 2016,
  "secretBase": "Super tower",
  "active": true,
  "members": [
    {
      "name": "Molecule Man",
      "age": 29,
      "secretIdentity": "Dan Jukes",
      "powers": [
        "Radiation resistance",
        "Turning tiny",
        "Radiation blast"
      ]
    },
    {
      "name": "Madame Uppercut",
      "age": 39,
      "secretIdentity": "Jane Wilson",
      "powers": [
        "Million tonne punch",
        "Damage resistance",
        "Superhuman reflexes"
      ]
    },
    {
      "name": "Eternal Flame",
      "age": 1000000,
      "secretIdentity": "Unknown",
      "powers": [
        "Immortality",
        "Heat Immunity",
        "Inferno",
        "Teleportation",
        "Interdimensional travel"
      ]
    }
  ]
}
    *
    * */



    public static void main(String[] args) throws AnalysisException {
        SparkSession session = SparkSession.builder().appName("Java Spark SQL basic example").master("local[*]").getOrCreate();

        runBasicDataFrameExamples(session); // DataFrame形式
        runSparkDataSetExample(session); // Datasets形式
        runInferSchemaExample(session);  // Spark SQL supports automatically converting an RDD of JavaBeans into a DataFrame

        runProgrammaticSchemaExample(session); // Programmatically Specifying the Schema
        session.stop();

    }



    private static void runInferSchemaExample(SparkSession spark) {
        // $example on:schema_inferring$
        // Create an RDD of Person objects from a text file
        JavaRDD<Company> peopleRDD = spark.read()
                .textFile("sparksql/data/company.txt")
                .javaRDD()
                .map(line -> {
                    String[] parts = line.split(",");
                    Company person = new Company();
                    person.setSquadName(parts[0]);
                    person.setHomeTown(parts[1]);
                    return person;
                });

        // Apply a schema to an RDD of JavaBeans to get a DataFrame
        Dataset<Row> peopleDF = spark.createDataFrame(peopleRDD, Company.class);
        // Register the DataFrame as a temporary view
        peopleDF.createOrReplaceTempView("people");

        // SQL statements can be run by using the sql methods provided by spark
        Dataset<Row> teenagersDF = spark.sql("SELECT squadName FROM people WHERE squadName like '%Super%' ");

        // The columns of a row in the result can be accessed by field index
        Encoder<String> stringEncoder = Encoders.STRING();
        Dataset<String> teenagerNamesByIndexDF = teenagersDF.map(
                (MapFunction<Row, String>) row -> "squadName: " + row.getString(0),
                stringEncoder);
        teenagerNamesByIndexDF.show();
        // +------------+
        // |       value|
        // +------------+
        // |Name: Justin|
        // +------------+

        // or by field name
        Dataset<String> teenagerNamesByFieldDF = teenagersDF.map(
                (MapFunction<Row, String>) row -> "squadName: " + row.<String>getAs("squadName"),
                stringEncoder);
        teenagerNamesByFieldDF.show();
        // +------------+
        // |       value|
        // +------------+
        // |Name: Justin|
        // +------------+
        // $example off:schema_inferring$
    }

    private static void runProgrammaticSchemaExample(SparkSession spark) {
        // $example on:programmatic_schema$
        // Create an RDD
        JavaRDD<String> peopleRDD = spark.sparkContext()
                .textFile("sparksql/data/company.txt", 1)
                .toJavaRDD();

        // The schema is encoded in a string
        String schemaString = "squadName hometown";

        // Generate the schema based on the string of schema
        List<StructField> fields = new ArrayList<>();
        for (String fieldName : schemaString.split(" ")) {
            StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
            fields.add(field);
        }
        StructType schema = DataTypes.createStructType(fields);

        // Convert records of the RDD (people) to Rows
        JavaRDD<Row> rowRDD = peopleRDD.map((Function<String, Row>) record -> {
            String[] attributes = record.split(",");
            return RowFactory.create(attributes[0], attributes[1].trim());
        });

        // Apply the schema to the RDD
        Dataset<Row> peopleDataFrame = spark.createDataFrame(rowRDD, schema);

        // Creates a temporary view using the DataFrame
        peopleDataFrame.createOrReplaceTempView("people");

        // SQL can be run over a temporary view created using DataFrames
        Dataset<Row> results = spark.sql("SELECT squadName, hometown  FROM people");

        results.show();

        // The results of SQL queries are DataFrames and support all the normal RDD operations
        // The columns of a row in the result can be accessed by field index or by field name
//        Dataset<String> namesDS = results.map(
//                (MapFunction<Row, String>) row -> "squadName: " + row.getString(0),
//                Encoders.STRING());
//        namesDS.show();
        // +-------------+
        // |        value|
        // +-------------+
        // |Name: Michael|
        // |   Name: Andy|
        // | Name: Justin|
        // +-------------+
        // $example off:programmatic_schema$
    }

    private static void runSparkDataSetExample(SparkSession session) {
        Dataset<Row> df = session.read().json("sparksql/data/company.json");

        Company company = new Company();
        company.setSquadName("");
        company.setHomeTown("town");

        // 从 Bean class中创建 Dataset
        Encoder<Company> companyEncoder = Encoders.bean(Company.class);
        Dataset<Company> companyDataset = session.createDataset(Collections.singletonList(company), companyEncoder);

//        companyDataset.show();

        // 也可以从基本数据类型创建dataset
        Encoder<Long> longEncoder = Encoders.LONG();
        Dataset<Long> primitiveDS = session.createDataset(Arrays.asList(1L, 2L, 3L), longEncoder);
        Dataset<Long> transformedDS = primitiveDS.map(
                (MapFunction<Long, Long>) value -> value + 1L,
                longEncoder);
        transformedDS.collect(); // Returns [2, 3, 4]

        // 将dataFrame转化为dataset
        String path = "sparksql/data/company.json";
        Dataset<Company> companyDatasetFromDataFrame = session.read().json(path).as(companyEncoder);

        companyDatasetFromDataFrame.select("members").show();

    }

    private static void runBasicDataFrameExamples(SparkSession session) throws AnalysisException {
        DataFrameReader read = session.read();
        Dataset<Row> company = read.json("sparksql/data/company.json");
//        company.printSchema();
//
//        company.show();

//        company.select("members").show();

        company.createOrReplaceTempView("company");
        Dataset<Row> members = session.sql("select * from company where members is not null");
//        members.show();


        // global temp view，默认存储在global_temp库中
        company.createGlobalTempView("companyGlobal");

        session.sql("select * from global_temp.companyGlobal").show();

    }
}

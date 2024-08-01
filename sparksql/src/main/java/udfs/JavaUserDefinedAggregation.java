package udfs;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.expressions.aggregate.Average;
import org.apache.spark.sql.expressions.Aggregator;
import org.apache.spark.sql.types.DataTypes;
import scala.Function1;
import scala.Function2;

import java.io.Serializable;

public class JavaUserDefinedAggregation {
    public static void main(String[] args) throws AnalysisException {

        SparkSession spark = SparkSession.builder()
                .appName("JavaUserDefinedAggregation")
                .master("local[*]")
                .getOrCreate();

        // udaf(c1, c2)
        // c1：实现udaf的类实例
        // c2: udaf入参类型
        spark.udf().register("myAverage", functions.udaf(new MyAverage(),Encoders.LONG()));

        Dataset<Row> source = spark.read().json("sparksql/data/employees.json");
        source.createTempView("employees");

        //+---------+
        //|avgSalary|
        //+---------+
        //|   3750.0|
        //+---------+
        spark.sql("select myAverage(salary) avgSalary from employees;").show();

        spark.stop();

    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Average implements Serializable {

        private Long sum;
        private Long count;

    }

    public static class MyAverage extends Aggregator<Long, Average, Double> {


        @Override
        public Average zero() {
            return new Average(0L, 0L);
        }

        @Override
        public Average reduce(Average b, Long a) {
            b.setCount(b.getCount() + 1L);
            b.setSum(b.getSum() + a);
            return b;
        }

        @Override
        public Average merge(Average b1, Average b2) {
            b1.setSum(b1.getSum() + b2.getSum());
            b1.setCount(b1.getCount() + b2.getCount());
            return b1;
        }

        @Override
        public Double finish(Average reduction) {
            return (reduction.getSum() / reduction.getCount()) * 1.0;
        }

        @Override
        public Encoder<Average> bufferEncoder() {
            return Encoders.bean(Average.class);
        }

        @Override
        public Encoder<Double> outputEncoder() {
            return Encoders.DOUBLE();
        }
    }
}

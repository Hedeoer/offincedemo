package cn.hedeoer.sharevariable;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.LongAccumulator;
import org.junit.Test;
import scala.Serializable;

import java.util.Arrays;
import java.util.List;

public class $01Broadcast_Variables implements Serializable {


    // 广播变量测试

    @Test
    public void broadcastVariabales() throws InterruptedException {

        JavaSparkContext sc = new JavaSparkContext(new SparkContext("local[*]", "bv"));
        JavaRDD<String> strRDD = sc.parallelize(Arrays.asList("hello world",
                "hello spark",
                "hello scala",
                "hello hadoop",
                "hello flink"
        ));

        // 不使用广播的情况
        // lucky string
        List<String> luckList = Arrays.asList("hello spark", "hello scala");

//        strRDD.filter(
//                new Function<String, Boolean>() {
//                    @Override
//                    public Boolean call(String v1) throws Exception {
//                        return  luckList.contains(v1);
//                    }
//                }
//        ).foreach(s -> System.out.println(s));

        // 使用广播的情况
        Broadcast<List<String>> luck = sc.broadcast(luckList);
        strRDD.filter(
                new Function<String, Boolean>() {
                    @Override
                    public Boolean call(String v1) throws Exception {
                        return  luck.value().contains(v1);
                    }
                }
        ).foreach(s -> System.out.println(s));

        Thread.sleep(10000000);
    }


    @Test
    public void accumulator() throws InterruptedException {
        JavaSparkContext sc = new JavaSparkContext(new SparkContext("local[*]", "bv"));

        JavaRDD<Integer> parallelize = sc.parallelize(Arrays.asList(1, 2, 3, 5, 6));

        LongAccumulator longAccumulator = sc.sc().longAccumulator();

        parallelize.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer v1) throws Exception {
                longAccumulator.add(v1);
                return v1;
            }
        }).collect();

        System.out.println(longAccumulator.value());

        Thread.sleep(10000000);

    }


}

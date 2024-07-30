package cn.hedeoer;


import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.junit.Test;
import scala.Serializable;

import java.util.Arrays;


public class $01RddCreate implements Serializable {
    @Test
    public void CreateRddByFile(){
        SparkContext conf = new SparkContext("local[*]", "CreateRddByFile");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 读取文件和通过其他rdd衍生
        // 主类需要实现 scala.Serializable
        JavaRDD<String> source = sc.textFile("data/text.properties");
        Integer totalLenth = source.map(new Function<String, Integer>() {
            @Override
            public Integer call(String v1) throws Exception {
                return v1.length();
            }
        }).reduce(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        System.out.println(totalLenth);
    }


    @Test
    public void CreateRddByCollect(){
        SparkContext conf = new SparkContext("local[*]", "CreateRddByFile");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 读取集合数据
        JavaRDD<Integer> parallelize = sc.parallelize(Arrays.asList(1, 4, 6, 5));
        parallelize.collect().forEach(System.out::println);
    }
}

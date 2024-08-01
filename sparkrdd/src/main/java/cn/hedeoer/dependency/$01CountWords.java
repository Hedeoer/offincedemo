package cn.hedeoer.dependency;

import cn.hedeoer.transformations.$01Transformations;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import scala.Tuple2;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Iterator;

public class $01CountWords {
    public static void main(String[] args) throws InterruptedException {

        JavaSparkContext sc = new JavaSparkContext(new SparkContext("local[*]", "CountWords"));

        JavaRDD<String> source = sc.textFile("sparkrdd/data/dim_video_full");

        source.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split("\t")).iterator();
            }
        }).mapToPair(s -> new Tuple2<>(s, 1))
                .reduceByKey((a, b) -> a + b)
                .foreach(tuple2 -> System.out.println(tuple2._1 + ":" + tuple2._2));


        Thread.sleep(99999999);

        /*
        *
        * application:sparkcontext的个数就是application的个数
        * job：job个数  = 行动算子个数
        * stage：按照宽依赖（shuffle）划分，stage  = shuffle次数 + 1
        * task：task个数 = 最后一个RDD的partition个数
        *
        * 上述列子中：
        * application：1个
        * job：1个
        * stage：2个
        * task个数：2个
        * */



    }
}

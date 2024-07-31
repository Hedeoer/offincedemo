package cn.hedeoer.actions;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.junit.Test;
import scala.Serializable;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;


public class $02Actions implements Serializable {

    /**
     * reduce算子
     * ①：单值类型数据， 相当于累计的形式
     * ②：kv对类型， 相当于对value的值的累计统计，key不同的话，只能覆盖
     * @throws InterruptedException
     */
    @Test
    public void reduce() throws InterruptedException {
        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setMaster("local[1]").setAppName("reduceTest"));

        JavaRDD<Integer> source = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
        // 单值类型
        Integer reduce = source.reduce(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        // 55
        System.out.println(reduce);


        //  kv对类型,创建kv类型
        List<Tuple2<String, Integer>> list = Arrays.asList(
                new Tuple2<String, Integer>("a", 1),
                new Tuple2<String, Integer>("a", 3),
                new Tuple2<String, Integer>("a", 4)
        );

        Tuple2<String, Integer> reduce1 = sc.parallelize(list)
                .reduce(new Function2<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> call(Tuple2<String, Integer> v1, Tuple2<String, Integer> v2) throws Exception {
                        return new Tuple2<>(v1._1, v1._2 + v2._2);
                    }
                });
        // (a,8)
        System.out.println(reduce1);


        Thread.sleep(999999999);
    }


    /**
     * collect算子
     * Return all the elements of the dataset as an array at the driver program
     */
    @Test
    public void collect(){
        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setMaster("local[1]").setAppName("collectTest"));
        JavaRDD<Integer> source = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));

        source.collect().forEach(System.out::print);
    }

    /**
     * count算子：
     * 统计数据源中元素的个数
     */
    @Test
    public void count(){
        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setMaster("local[1]").setAppName("countTest"));
        // textFile读取文件，默认是按行读取，一行封装为一个元素
        JavaRDD<String> source = sc.textFile("data/dim_video_full");

        // 输出的是dim_video_full文件的行数
        System.out.println(source.count());
    }


    /**
     * take算子
     * 按照RDD的分区顺序，返回前n个元素到driver的内存中， 当返回的数据量大时，推荐使用collect()
     */
    @Test
    public void take(){
        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setMaster("local[*]").setAppName("takeTest"));
        JavaRDD<String> source = sc.textFile("data/dim_video_full", 3);

        // 获取RDD中前4个元素
        List<String> take = source.take(4);
        take.forEach(System.out::println);

        // 返回第一个元素 相当于 take（1）
        System.out.println(source.first());
    }


    /**
     * takeSample算子
     * withReplacement: 布尔值，表示是否允许重复抽样。如果为 true，则可能有相同的元素被多次抽取；如果为 false，则保证抽取的样本不重复。
     * num: 抽取样本的数量。这表示你想要从数据集中抽取多少个元素作为样本。
     * seed: 长整型值，作为随机数生成器的种子。设置种子可以确保在相同条件下运行时能够得到相同的随机样本。
     */
    @Test
    public void taskSample(){
        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setMaster("local[*]").setAppName("takeTest"));
        JavaRDD<String> source = sc.textFile("data/dim_video_full", 3);

        List<String> prefix = source.map(a -> a.split("\t")[0]).takeSample(false, 4, 1);
        prefix.forEach(System.out::println);
    }


    /**
     * saveAsTextFile算子,
     * 最终的文件的个数由输入RDD的分区数决定
     */
    @Test
    public void saveAsTextFile(){
        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setMaster("local[1]").setAppName("takeTest"));
        // textFile算子，最终的分区数为（默认的2个分区和环境可用核数的最小值）
        JavaRDD<String> source = sc.textFile("data/dim_video_full" );
        // 分区数为1，最终结果在一个文件中
        source.map(a -> a.split("\t")[7])
                .distinct()
                .saveAsTextFile("data/dim_video_full_save");


        // 模拟saveASTextFile输出到多个文件
        source.map(a -> a.split("\t")[7])
                .distinct()
                .mapToPair(a -> new Tuple2<>(a, 1))
                .groupByKey(3)
                .saveAsTextFile("data/dim_video_full_save/multi");

    }


    /**
     *takeOrdered算子
     * 按照顺序（可排序按照自然顺序， 可以自定义比较器指定排序）
     * 自定义比较器的注意事项：1. 必须实现 scala.Serializable 接口 2. 必须实现 java.util.Comparator 或者 java.lang.Comparable 接口
     */
    @Test
    public void takeOrdered(){
        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setMaster("local[*]").setAppName("takeTest"));
        JavaRDD<String> source = sc.textFile("data/dim_video_full", 3);

        // SerializableComparator自定义接口实现了 实现 scala.Serializable 接口 ， java.util.Comparator接口
        // 否则 报序列化问题
        // 按照章节名字的长度升序排序取前两个
        source.map(a -> a.split("\t")[7]).distinct()
                .takeOrdered(2, new SerializableComparator<String>() {
                    @Override
                    public int compare(String o1, String o2) {
                        return o1.length() - o2.length();
                    }
                })
                .forEach(System.out::println);

        sc.stop();

    }

    /**
     * countByKey算子
     * 按照key，统计key出现的次数
     */

    @Test
    public void  countByKey(){
        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setMaster("local[*]").setAppName("takeTest"));

        //  kv对类型,创建kv类型
        List<Tuple2<String, Integer>> list = Arrays.asList(
                new Tuple2<String, Integer>("a", 1),
                new Tuple2<String, Integer>("a", 3),
                new Tuple2<String, Integer>("a", 4),
                new Tuple2<String, Integer>("b", 4),
                new Tuple2<String, Integer>("a", 4),
                new Tuple2<String, Integer>("b", 4)
        );


        JavaPairRDD<String, Integer> pair = sc.parallelize(list).mapToPair(a -> new Tuple2<>(a._1, 1));
        pair.countByKey().forEach((k, v) -> System.out.println(k + ":" + v));

        // a:4
        //b:2

    }







}

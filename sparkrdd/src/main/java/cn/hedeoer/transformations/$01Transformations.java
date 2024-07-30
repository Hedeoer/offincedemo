package cn.hedeoer.transformations;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.junit.Test;
import scala.Serializable;
import scala.Tuple2;
import scala.Tuple3;

import java.util.*;
import java.util.stream.Collectors;

public class $01Transformations implements Serializable {
    @Test
    public void map() throws InterruptedException {
        // 1. 创建sparkSession
        // 2. 创建sparkContext/
        // 3. 创建rdd
        // 4. map
        // 5. collect
        // 6. 关闭sparkSession

        SparkContext conf = new SparkContext("local[*]", "CreateRddByFile");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 读取文件和通过其他rdd衍生
        JavaRDD<String> source = sc.textFile("data/dim_video_full");

        // 使用fliter算子过滤掉首行
        // 使用map算子（id， duration_sec）转换为map,并将每个duration_sec + 1
        // collect 收集结果并打印
        source.filter(v1 -> !v1.contains("id"))
                .map(new Function<String, Map<String, Integer>>() {
                    @Override
                    public Map<String, Integer> call(String v1) throws Exception {
                        String id = v1.split("\t")[0];
                        String durationSec = v1.split("\t")[2];
                        Integer convertSec = Integer.valueOf(durationSec) + 1;
                        HashMap<String, Integer> kv = new HashMap<>();
                        kv.put(id, convertSec);

                        return kv;

                    }
                }).collect()
                .forEach(kv -> {
                    kv.forEach((k, v) -> {
                        System.out.println(k + " : " + v);
                    });
                });


        Thread.sleep(9999999999999L);


    }


    @Test
    public void flatmap() {
        SparkContext conf = new SparkContext("local[*]", "CreateRddByFile");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 读取文件和通过其他rdd衍生
        JavaRDD<String> source = sc.textFile("data/dim_video_full");

        JavaRDD<String> stringJavaRDD = source.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                String[] line = s.split("\t");
                ArrayList<String> videoMessage = new ArrayList<>();
                videoMessage.add(line[6]);
//                videoMessage.add(line[7]);
                return videoMessage.iterator();
            }
        });
        stringJavaRDD.collect().forEach(System.out::println);
    }


    @Test
    public void groupBy() {
        SparkContext conf = new SparkContext("local[*]", "CreateRddByFile");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 读取文件和通过其他rdd衍生
        JavaRDD<String> source = sc.textFile("data/dim_video_full");

        // 按照时长进行3分组
        // group by 返回的类型为 JavaPairRDD[K, Iterable[v]]
        JavaPairRDD<Integer, Iterable<Integer>> idGroups = source.filter(v1 -> !v1.contains("id"))
                .map(line -> {
                    return Integer.valueOf(line.split("\t")[2]);
                })
                .groupBy(new Function<Integer, Integer>() {
                    @Override
                    public Integer call(Integer v1) throws Exception {
                        return v1 % 3;
                    }
                });
        idGroups.collect().forEach(System.out::println);

/*
* 打印结果
(0,[300, 1200, 900, 600, 1500, 900, 1050, 1200, 900, 600, 750, 900, 600, 750])
(2,[800, 1100, 950, 1400, 800, 800, 650])
(1,[1000, 700, 1300, 850, 1150, 1000, 700, 850, 700])
* */
    }


    /**
     * distinct 去重算子对视频状态去重
     * 具体的去重规则按照 对象本身的equals方法和hashcode()进行判断
     */
    @Test
    public void distinct() {
        SparkContext conf = new SparkContext("local[*]", "CreateRddByFile");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 读取文件和通过其他rdd衍生
        JavaRDD<String> source = sc.textFile("data/dim_video_full");
        source.filter(v1 -> !v1.contains("id"))
                .map(line -> {
                    return line.split("\t")[3];
                })
                .distinct()
                .collect()
                .forEach(System.out::println);

    }
/*
* 结果
* 未上传
上传完
上传中
* */

    /**
     * mapPartitions算子: 按照传入的RDD的分区数执行，分区数等于迭代器的数量。一些共有资源不适合重复创建的场景可以使用mapParttions算子，比如一些链接的线程资源
     * 相比map，每条记录都需要执行一次逻辑运行。
     */
    @Test
    public void mapPartitions() {
        JavaSparkContext sc = new JavaSparkContext(new SparkContext("local[*]", "MappPartition"));
        // 指定通过集合创建的RDD的分区数为2
        JavaRDD<Integer> source = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 2);


        source.mapPartitions(new FlatMapFunction<Iterator<Integer>, Integer>() {
            @Override
            public Iterator<Integer> call(Iterator<Integer> integerIterator) throws Exception {
                System.out.println("===========>");
                return integerIterator;
            }
        }).collect();
    }

/*
* 执行结果，可知传入的RDD分区为2，并且只执行了2次
===========>
===========>
* */

    /**
     * mapPartitionsWithIndex算子:相比mapPartitions算子，保留了分区索引，分区从0开始
     */
    @Test
    public void mapPartitionsWithIndex() {
        JavaSparkContext sc = new JavaSparkContext(new SparkContext("local[*]", "MappPartition"));
        // 指定通过集合创建的RDD的分区数为2
        JavaRDD<Integer> source = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 2);


        source.mapPartitionsWithIndex(new Function2<Integer, Iterator<Integer>, Iterator<Integer>>() {
            @Override
            public Iterator<Integer> call(Integer v1, Iterator<Integer> v2) throws Exception {
                System.out.println("===========>" + v1);
                return v2;
            }
        }, true).collect();
    }
    // 结果
//===========>1
//===========>0


    /**
     * sample算子: 随机抽取数据，参数1为是否放回，参数2为抽取数据的比例
     * 用途：当数据倾斜出现时对数据抽样分析
     */
    @Test
    public void sample() {
        JavaSparkContext sc = new JavaSparkContext(new SparkContext("local[*]", "MappPartition"));
        // 指定通过集合创建的RDD的分区数为2
        JavaRDD<Integer> source = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 2);

        source.sample(false, 0.5).collect().forEach(System.out::println);
    }


    /**
     * union算子: 合并两个RDD
     * 不对合并的数据去重，需要去重需要结合 distinct算子
     */
    @Test
    public void union() {

        JavaSparkContext sc = new JavaSparkContext(new SparkContext("local[*]", "MappPartition"));
        // 指定通过集合创建的RDD的分区数为2
        JavaRDD<Integer> source1 = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 2);
        JavaRDD<Integer> source2 = sc.parallelize(Arrays.asList(10));

        source1.union(source2).collect().forEach(System.out::println);

    }

    /**
     * intersection算子: 求两个RDD的交集,交集部分出现元素重复只会输出一次
     */
    @Test
    public void intersection() {
        JavaSparkContext sc = new JavaSparkContext(new SparkContext("local[*]", "MappPartition"));
        // 指定通过集合创建的RDD的分区数为2
        JavaRDD<Integer> source1 = sc.parallelize(Arrays.asList(1, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 2);
        JavaRDD<Integer> source2 = sc.parallelize(Arrays.asList(10, 1, 1));

        source1.intersection(source2).collect().forEach(System.out::println);
    }

    /**
     * groupByKey算子: 按照key进行分组，返回类型为JavaPairRDD[K, Iterable[V]]
     * 只有k v对类型的RDD才能调用groupByKey算子\
     * <p>
     * 和 groupBy的区别
     * 1. groupByKey算子是针对key进行分组，groupBy算子可以对任意类型的RDD进行分组
     */
    @Test
    public void groupByKey() {
        // 每个章节有那些视频？
        SparkContext conf = new SparkContext("local[*]", "CreateRddByFile");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 读取文件和通过其他rdd衍生
        JavaRDD<String> source = sc.textFile("data/dim_video_full");
        source.mapToPair(new PairFunction<String, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(String s) throws Exception {
                        String[] fields = s.split("\t");

                        return new Tuple2<>(fields[7], 1);
                    }
                }).groupByKey()
                .collect()
                .forEach(System.out::println);
    }


    /**
     * reduceByKey算子: 按照key进行分组聚合，在map阶段进行预聚合，相当于mapreduce中的combiner。减少了shuffle阶段传输的数据量
     * 于reduceByKey比较：
     * 1. reduceByKey只进行分组，reduceByKey进行分组聚合
     * 2.reduceByKey减少了shuffle阶段传输的数据量
     */
    @Test
    public void reduceByKey() {
        // 每个章节有几个视频？
        SparkContext conf = new SparkContext("local[*]", "CreateRddByFile");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 读取文件和通过其他rdd衍生
        JavaRDD<String> source = sc.textFile("data/dim_video_full");
        source.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                String[] fields = s.split("\t");

                return new Tuple2<>(fields[7], 1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        }).collect().forEach(System.out::println);
    }


    /**
     * aggregateByKey算子: 按照key进行聚合，返回类型为JavaPair， 如果想要分组聚合，使用aggregateByKey算子比groupByKey算子性能更高
     * 参数说明：
     * zeroValue: U, 聚合起始值，比如累加
     * partitioner: Partitioner, 分区规则
     * seqFunc: Function2[U, V, U], 每个分区的聚合规则
     * Function2[U, U, U]): JavaPairRDD[K, U] 不同节点相同分区的数据聚合规则
     */
    @Test
    public void aggregateByKey() {
        // 每个章节有几个视频？
        SparkContext conf = new SparkContext("local[*]", "CreateRddByFile");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 读取文件和通过其他rdd衍生
        JavaRDD<String> source = sc.textFile("data/dim_video_full");
        source.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                String[] fields = s.split("\t");

                return new Tuple2<>(fields[7], 1);
            }
        }).aggregateByKey(0,
                new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer v1, Integer v2) throws Exception {
                        return v1 + v2;
                    }
                }, new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer v1, Integer v2) throws Exception {
                        return v1 + v2;
                    }
                }).collect().forEach(System.out::println);
    }
    // 结果
//(Data Analysis,2)
//(Databases,6)
//(Data Structures,4)
//(Advanced Topics,6)
//(Introduction,6)
//(Web Development,6)
//(7_chapter_name,1)

    /**
     * sortByKey算子: 按照key进行排序，默认是升序，如果要降序，需要使用sortByKey(false)
     * 要求key可排序，
     */
    @Test
    public void sortByKey() {
        // 按照发布视频数量对各个章节倒序排列
        SparkContext conf = new SparkContext("local[*]", "CreateRddByFile");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 读取文件和通过其他rdd衍生
        JavaRDD<String> source = sc.textFile("data/dim_video_full");
        // 转化为pair类型
        source.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                String[] fields = s.split("\t");

                return new Tuple2<>(fields[7], 1);
            }
            // 按照章节名字进行分组聚合
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        }
        // kv对倒序转化，《章节名，视频数》==》 《视频数,章节名》
        ).mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return new Tuple2<>(stringIntegerTuple2._2, stringIntegerTuple2._1);
            }
            // 按照视频数量倒序排序输出
        }).sortByKey(false)
                .collect()
                .forEach(System.out::println);
    }


    /**
     * 为每个章节信息补全课程信息
     * join算子: 按照key进行连接,类似sql中的join
     * 类似的算子还有有leftOuterJoin,rightOuterJoin,fullOuterJoin
     */
    @Test
    public void join(){

        SparkContext conf = new SparkContext("local[*]", "CreateRddByFile");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 课程名，章节名，视频数
        ArrayList<Tuple3<String,String, Integer>> chapter = new ArrayList<>();
        chapter.add(new Tuple3<>("SC","Data Analysis", 2));
        chapter.add(new Tuple3<>("SC","Databases", 6));
        chapter.add(new Tuple3<>("SC","Data Structures", 4));

        // 课程名，课程编号
        ArrayList<Tuple2<String, Integer>> courseList = new ArrayList<>();
        courseList.add(new Tuple2<>("SC", 100));
        courseList.add(new Tuple2<>("CN",200));
        courseList.add(new Tuple2<>("EU", 300));


        // <课程名,<章节名,视频数>>
        JavaPairRDD<String, Tuple2<String, Integer>> chapterStream = sc.parallelizePairs(chapter.stream()
                .map(x -> new Tuple2<>(x._1(), new Tuple2<>(x._2(), x._3())))
                .collect(Collectors.toList()));

        // <课程名，课程编号>
        JavaPairRDD<String, Integer> courseSteam = sc.parallelizePairs(courseList);

        JavaPairRDD<String, Tuple2<Tuple2<String, Integer>, Integer>> result = chapterStream.join(courseSteam);
        result.foreach(new VoidFunction<Tuple2<String, Tuple2<Tuple2<String, Integer>, Integer>>>() {
            @Override
            public void call(Tuple2<String, Tuple2<Tuple2<String, Integer>, Integer>> joinRows) throws Exception {
                // 课程名，课程编号，章节名，视频数
                System.out.println(joinRows._1 + "," + joinRows._2._2 + "," + joinRows._2._1._1 + "," + joinRows._2._1._2 );
            }
        });


    }
    // 结果
//SC,100,Data Analysis,2
//SC,100,Databases,6
//SC,100,Data Structures,4


    /**
     * cartesian算子: 笛卡尔积，将两个rdd的元素进行组合
     */
    @Test
    public void cartesian(){
        // local【1】，本地串行执行
        SparkContext conf = new SparkContext("local[1]", "CreateRddByFile");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Integer> s1 = sc.parallelize(Arrays.asList(1, 3));
        JavaRDD<Integer> s2 = sc.parallelize(Arrays.asList(2, 6));

        s1.cartesian(s2).foreach(x -> System.out.println(x._1 + ":" + x._2) );
    }
//1:2
//1:6
//3:2
//3:6

    /**
     * Pipe each partition of the RDD through a shell command, e.g. a Perl or bash script.
     * RDD elements are written to the process's stdin
     * and lines output to its stdout are returned as an RDD of strings.
     */
    @Test
    public void pip(){

    }

    /**
     * coalesce算子: 调整分区
     * 推荐博客 https://lilinchao.com/archives/1300.html 有关coalesce的部分
     */
    @Test
    public void coalesce(){

    }

    /**
     * 扩大分区
     */
    @Test
    public void rePartition(){

    }

    /**
     * repartitionAndSortWithinPartitions是Spark官网推荐的一个算子，
     * 官方建议，如果需要在repartition重分区之后，还要进行sort 排序，建议直接使用repartitionAndSortWithinPartitions算子。
     * 因为该算子可以一边进行重分区的shuffle操作，一边进行排序。
     * shuffle与sort两个操作同时进行，比先shuffle再sort来说，性能可能是要高的。
     */
    @Test
    public void  repartitionAndSortWithinPartitions(){

    }


}

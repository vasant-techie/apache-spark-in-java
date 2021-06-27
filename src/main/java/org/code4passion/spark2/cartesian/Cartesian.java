package org.code4passion.spark2.cartesian;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class Cartesian {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("Cartesian");
        JavaSparkContext context = new JavaSparkContext(conf);

        JavaRDD<String> nameRDD = context.parallelize(Arrays.asList("A", "B", "C"));
        JavaRDD<Integer> numberRDD = context.parallelize(Arrays.asList(1, 2, 3));
        JavaPairRDD<String, Integer> cartesian = nameRDD.cartesian(numberRDD);
        List<Tuple2<String, Integer>> result = cartesian.collect();
        result.stream().forEach(rec -> System.out.println(rec._1 + ":" + rec._2));
    }
}

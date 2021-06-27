package org.code4passion.spark2.flatMapToPair;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.stream.Collectors;

public class WordLetterCount {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("Word-Letter-Count");
        JavaSparkContext context = new JavaSparkContext(conf);

        JavaRDD<String> stringRDD = context.parallelize(Arrays.asList("Hello Java", "Hello Groovy"));
        JavaPairRDD<String, Integer> letterCountPairRDD = stringRDD.flatMapToPair(word ->
                Arrays.asList(word.split(" ")).stream()
                        .map(token ->
                                new Tuple2<String, Integer>(token, token.length())
                        ).collect(Collectors.toList()).iterator());

        letterCountPairRDD.saveAsTextFile("letterCount.txt");

    }
}

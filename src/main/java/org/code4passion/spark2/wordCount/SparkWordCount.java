package org.code4passion.spark2.wordCount;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class SparkWordCount {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("WordCount");

        JavaSparkContext javaSparkContext = new JavaSparkContext(conf);

        //JavaRDD<String> inputData = javaSparkContext.textFile("/usr/local/Cellar/apache-spark/2.4.3/libexec/examples/src/main/resources/people.txt");
        JavaRDD<String> inputData
                =javaSparkContext.parallelize(Arrays.asList("where there is a will there is a way"));

        JavaPairRDD<String, Integer> flattenPairs = inputData.flatMapToPair(text ->
                Arrays.asList(text.split(" ")).stream().map(
                        word -> new Tuple2<String, Integer>(word, 1)).iterator());

        JavaPairRDD<String, Integer> wordCountRDD = flattenPairs.reduceByKey((v1, v2) -> v1+v2);
        wordCountRDD.saveAsTextFile("wordCount.txt");
    }
}

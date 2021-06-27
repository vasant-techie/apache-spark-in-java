package org.code4passion.spark2.union;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

public class Union {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("Union");
        JavaSparkContext context = new JavaSparkContext(conf);
        JavaRDD<String> names = context.parallelize(Arrays.asList("Murugan", "Vinayagamoorthy"));
        JavaRDD<String> names2 = context.parallelize(Arrays.asList("Manivannan", "Prabhakaran"));
        JavaRDD<String> union = names.union(names2);
        System.out.println(union.count());
    }
}

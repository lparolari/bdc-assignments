package it.unipd.bdc.ay2019.group49.hw;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.IOException;
import java.util.*;

public class G49HW1 {

    /*
    ASSIGNMENT
    ==========

    You must write a program GxxHW1.java (for Java users) or GxxHW1.py (for Python users),
    where xx is your two-digit group number, which receives in input an integer K and path to a text
    file containing a collection of pairs (i,gamma_i). The text file stores one pair per line which
    contains the key i (a progressive integer index starting from 0) and the value gamma_i (a string)
    separated by single space, with no parentheses or comma. (Note that while in the problem defined
    in the slides the i-th input pair contained also an object o_i, here the objects are disregarded.)
    */

    public static void main(String[] args) throws IOException {
        if (args.length != 2) {
            throw new IllegalArgumentException("USAGE: num_partitions file_path");
        }

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // SPARK SETUP
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        SparkConf conf = new SparkConf(true).setAppName("Homework1").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN");

        // Input parameters
        int partitionNo = Integer.parseInt(args[0]);
        String datasetPath = args[1];

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // BASIC CLASS COUNT ALGORITHMS
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        System.out.println();
        classCountDeterministicPartition(sc, partitionNo, datasetPath);
        classCountSparkPartitions(sc, datasetPath);
    }

    public static void classCountDeterministicPartition(JavaSparkContext sc, final int K, String path) {
        System.out.println("VERSION WITH DETERMINISTIC PARTITIONS");

        JavaRDD<String> elementsRDD = sc.textFile(path).repartition(K);

        JavaPairRDD<String, Long> countClass;
        countClass = elementsRDD
                .flatMapToPair((element) -> {
                    String[] tokens = element.split(" ");
                    ArrayList<Tuple2<Integer, Tuple2<String, String>>> pairs = new ArrayList<>();
                    pairs.add(new Tuple2<>(
                            Integer.parseInt(tokens[0]) % 1000, new Tuple2<>(tokens[0], tokens[1])));
                    return pairs.iterator();
                })
                .groupByKey()
                .flatMapToPair((triplet) -> {
                    HashMap<String, Long> counts = new HashMap<>();
                    for (Tuple2<String, String> c : triplet._2()) {
                        counts.put(c._2(), 1L + counts.getOrDefault(c._2(), 0L));
                    }
                    ArrayList<Tuple2<String, Long>> pairs = new ArrayList<>();
                    for (Map.Entry<String, Long> e : counts.entrySet()) {
                        pairs.add(new Tuple2<>(e.getKey(), e.getValue()));
                    }
                    return pairs.iterator();
                })
                .groupByKey() // <-- REDUCE PHASE (R2)
                .mapValues((it) -> {
                    long sum = 0;
                    for (long c : it) {
                        sum += c;
                    }
                    return sum;
                });

        System.out.println(countClass.collectAsMap());
    }

    public static void classCountSparkPartitions(JavaSparkContext sc, String path) {
        System.out.println("VERSION WITH SPARK PARTITIONS");
    }
}
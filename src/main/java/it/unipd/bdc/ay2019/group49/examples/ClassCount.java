package it.unipd.bdc.ay2019.group49.examples;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.IOException;
import java.util.*;

public class ClassCount {
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
    int K = Integer.parseInt(args[0]);

    // Read input file and subdivide it into K random partitions
    JavaRDD<String> elementsRDD = sc.textFile(args[1]).repartition(K);

    // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
    // BASIC CLASS COUNT ALGORITHM
    // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

    long elements = elementsRDD.count();
    System.out.println("Number of elements = " + elements);
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
    countClass.collectAsMap().forEach(
        (key, value) -> { System.out.println("class " + key + " has " + value + " occurences"); });
  }
}
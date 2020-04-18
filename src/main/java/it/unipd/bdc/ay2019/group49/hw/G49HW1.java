package it.unipd.bdc.ay2019.group49.hw;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

public class G49HW1 {
    /*
     * ASSIGNMENT ==========
     *
     * You must write a program GxxHW1.java (for Java users) or GxxHW1.py (for
     * Python users), where xx is your two-digit group number, which receives in
     * input an integer K and path to a text file containing a collection of pairs
     * (i,gamma_i). The text file stores one pair per line which contains the key i
     * (a progressive integer index starting from 0) and the value gamma_i (a
     * string) separated by single space, with no parentheses or comma. (Note that
     * while in the problem defined in the slides the i-th input pair contained also
     * an object o_i, here the objects are disregarded.)
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
        classCountSparkPartitions(sc, partitionNo, datasetPath);
    }

    public static void classCountDeterministicPartition(JavaSparkContext sc, final int K, String path) {
        System.out.println("VERSION WITH DETERMINISTIC PARTITIONS");

        JavaRDD<String> elementsRDD = sc.textFile(path).repartition(K);

        JavaPairRDD<String, Long> countClass;
        countClass = elementsRDD.flatMapToPair((element) -> {
            String[] tokens = element.split(" ");
            ArrayList<Tuple2<Integer, Tuple2<String, String>>> pairs = new ArrayList<>();
            pairs.add(new Tuple2<>(Integer.parseInt(tokens[0]) % K, new Tuple2<>(tokens[0], tokens[1])));
            return pairs.iterator();
        }).groupByKey().flatMapToPair((triplet) -> {
            HashMap<String, Long> counts = new HashMap<>();
            for (Tuple2<String, String> c : triplet._2())
                counts.put(c._2(), 1L + counts.getOrDefault(c._2(), 0L));
            ArrayList<Tuple2<String, Long>> pairs = new ArrayList<>();
            for (Map.Entry<String, Long> e : counts.entrySet()) {
                pairs.add(new Tuple2<>(e.getKey(), e.getValue()));
            }
            return pairs.iterator();
        }).groupByKey() // <-- REDUCE PHASE (R2)
                .mapValues((it) -> {
                    long sum = 0;
                    for (long c : it)
                        sum += c;
                    return sum;
                });

        Map<String, Long> sortedMap = new TreeMap<>(countClass.collectAsMap());
        System.out.println("Output pairs = " + sortedMap);
    }

    // version with statistics computed inside mapReduce algorithm
    public static void classCountSparkPartitions(JavaSparkContext sc, final int K, String path) {
        final String MAX_PARTITION_SIZE = "maxPartitionSize";

        System.out.println("VERSION WITH SPARK PARTITIONS");

        JavaRDD<String> elementsRDD = sc.textFile(path).repartition(K);

        JavaPairRDD<String, Long> countClass;
        countClass = elementsRDD.flatMapToPair((element) -> {
            String[] tokens = element.split(" ");
            ArrayList<Tuple2<Long, String>> pairs = new ArrayList<>();
            pairs.add(new Tuple2<>(Long.parseLong(tokens[0]), tokens[1]));
            return pairs.iterator();
        }).mapPartitionsToPair((cc) -> {
            long elementsProcessedByReducer = 0;
            HashMap<String, Long> counts = new HashMap<>();
            while (cc.hasNext()) {
                Tuple2<Long, String> currentClassName = cc.next();
                counts.put(currentClassName._2(), 1L + counts.getOrDefault(currentClassName._2(), 0L));
                elementsProcessedByReducer++;
            }
            // Build array of pairs (class, count(class)) to pass to the next round
            ArrayList<Tuple2<String, Long>> pairs = new ArrayList<>();
            for (Map.Entry<String, Long> e : counts.entrySet())
                pairs.add(new Tuple2<>(e.getKey(), e.getValue()));

            // Calculate maxPartitionSize
            pairs.add(new Tuple2<>(MAX_PARTITION_SIZE, elementsProcessedByReducer));
            return pairs.iterator();

        }).groupByKey().flatMapToPair((cc) -> {
            List<Tuple2<String, Long>> pairs = new ArrayList<>();
            Iterator<Long> elementsIterator = cc._2().iterator();

            if (cc._1().equals(MAX_PARTITION_SIZE)) {
                long max = 0;
                while (elementsIterator.hasNext())
                    max = Math.max(max, elementsIterator.next());
                pairs.add(new Tuple2<>(MAX_PARTITION_SIZE, max));
            } else {
                long sum = 0;
                while (elementsIterator.hasNext())
                    sum += elementsIterator.next();
                pairs.add(new Tuple2<>(cc._1(), sum));
            }
            return pairs.iterator();
        });

        Map<String, Long> sortedMap = new TreeMap<>(countClass.collectAsMap());
        Long maxPartion = sortedMap.get(MAX_PARTITION_SIZE);
        sortedMap.remove(MAX_PARTITION_SIZE);
        Entry<String, Long> maxValue = Collections.max(sortedMap.entrySet(),
                Comparator.comparingLong(Map.Entry::getValue)); // find class with max count

        System.out.println("Most frequent class = pair (" + maxValue.getKey() + "," + maxValue.getValue() + ") "
                + "with max count");
        System.out.println("Max partition size = " + maxPartion);
    }

    // version with statistics computed by hand
    public static void classCountSparkPartitionsv2(JavaSparkContext sc, final int K, String path) {
        JavaRDD<String> elementsRDD = sc.textFile(path).repartition(K);
        final String MAX_PARTITION_SIZE = "maxPartitionSize";

        JavaPairRDD<String, Iterable<Long>> c2 = elementsRDD.flatMapToPair((element) -> {
            String[] tokens = element.split(" ");
            ArrayList<Tuple2<Long, String>> pairs = new ArrayList<>();
            pairs.add(new Tuple2<>(Long.parseLong(tokens[0]), tokens[1]));
            return pairs.iterator();
        }).mapPartitionsToPair((cc) -> {
            long elementsProcessedByReducer = 0;
            HashMap<String, Long> counts = new HashMap<>();
            while (cc.hasNext()) {
                Tuple2<Long, String> currentClassName = cc.next();
                counts.put(currentClassName._2(), 1L + counts.getOrDefault(currentClassName._2(), 0L));
                elementsProcessedByReducer++;
            }
            // Build array of pairs (class, count(class)) to pass to the next round
            ArrayList<Tuple2<String, Long>> pairs = new ArrayList<>();
            for (Map.Entry<String, Long> e : counts.entrySet())
                pairs.add(new Tuple2<>(e.getKey(), e.getValue()));

            // Calculate maxPartitionSize
            pairs.add(new Tuple2<>(MAX_PARTITION_SIZE, elementsProcessedByReducer));
            return pairs.iterator();

        }).groupByKey();
        /*
         * stop with groupByKey(), in order to calculate statistics outside of MR
         * algorithm
         */

        Map<String, Iterable<Long>> sortedMap_ = new TreeMap<>(c2.collectAsMap());
        Map<String, Long> finalmap = new TreeMap<>(); // map that will contain the total count for each class
        Iterator<Long> iterator_max = sortedMap_.get(MAX_PARTITION_SIZE).iterator();
        long max_ = 0;

        while (iterator_max.hasNext()) { // find the max partition size
            Long current = iterator_max.next();
            max_ = Math.max(current, max_);
        }
        /*
         * remove MAX_PARTITION_SIZE in order to make max partition size not
         * interferring when finding the max class count
         */
        sortedMap_.remove(MAX_PARTITION_SIZE);

        sortedMap_.keySet().forEach(s -> { // for each class, sum all the entries to get total count for each class
            Iterator<Long> it = sortedMap_.get(s).iterator();
            long sum = 0;
            while (it.hasNext())
                sum += it.next();
            finalmap.put(s, sum);
        });
        Entry<String, Long> maxValue = Collections.max(finalmap.entrySet(), // find class with max count
                Comparator.comparingLong(Map.Entry::getValue));
        System.out.println("Most frequent class = pair (" + maxValue.getKey() + "," + maxValue.getValue() + ") "
                + "with max count");
        System.out.println("max partition: " + max_);
    }
}

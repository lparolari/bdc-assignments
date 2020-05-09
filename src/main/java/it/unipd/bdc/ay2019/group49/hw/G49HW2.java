package it.unipd.bdc.ay2019.group49.hw;

import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class G49HW2 {

    // Group 49: Luca Parolari, Giulio Piva
    // Homework 2, Big Data Computing
    // Assigned: 26/04/20, Deadline: 17/05/20

    // See [the homework page](http://www.dei.unipd.it/~capri/BDC/homeworks.htm)
    // and [the homework 2 page](http://www.dei.unipd.it/~capri/BDC/homework2.htm).

    public static final long SEED = 1236601;  // my university id

    // HOMEWORK ALGORITHMS
    // ===================

    /**
     * Compute the exact solution of maximum pairwise problem on the given input set `S`.
     *
     * @param S A set of input points.
     * @return The exact maximum distance between two points in `S`.
     */
    public static Double exactMPD(List<Vector> S) {
        /*
        Note: a very simple optimization for the exactMPD algorithm is to compare the points once.

        Due to the symmetric property of the distance metric d(p1, p2) = d(p2, p1), so we can
        skip some checks.

        The following two lines are the (more expressive) naive algorithm.
        ```
        for (Vector p1 : S)
            for (Vector p2 : S)
                res = max( res, d(p1, p2) )
        ```

        For aircraft-mainland:

        EXACT ALGORITHM (NAIVE, without optimization)
        Max distance = 3327.886194830245
        Running time = 4461

        EXACT ALGORITHM (with optimization)
        Max distance = 3327.886194830245
        Running time = 2337
        */

        double md = 0d;  // max distance between points, initially set to 0.

        for (int i = 0; i < S.size(); i++) {
            for (int j = i + 1; j < S.size(); j++) {
                Vector p1 = S.get(i);
                Vector p2 = S.get(j);
                md = Math.max(md, distance(p1, p2));
            }
        }

        return Math.sqrt(md);
    }

    /**
     * Compute the 2-approximated solution for the maximum pairwise problem selecting `k` points at
     * random from the given input set `S`.
     *
     * @param S A set of input points.
     * @param k The number of points to select at random from `S`.
     * @return The 2-approximated maximum distance between two points in `S`.
     */
    public static Double twoApproxMPD(List<Vector> S, Integer k) {
        Random generator = new Random(SEED);

        // TODO: review this algorithm.

        // Collect `k` random centers.
        List<Vector> C = IntStream.range(0, k)
                .map(i -> generator.nextInt(S.size()))
                .mapToObj(S::get)
                .collect(Collectors.toList());

        double md = 0d;

        for (Vector p1 : S) {
            for (Vector p2 : C) {
                md = Math.max(md, distance(p1, p2));
            }
        }

        return Math.sqrt(md);
    }

    /**
     * Compute the exact solution for the maximum pairwise problem on a subset of C of S,
     * where C are k centers returned by Farthest-First Traversal.
     *
     * @param S A set of input points.
     * @param k The number of points to select at random from `S`.
     * @return The exact maximum distance between two points in a subset of C of S,
     * where C are k centers returned by Farthest-First Traversal;
     */
    public static Double kCenterMPD(List<Vector> S, Integer k) {
        return exactMPD(farthestFirstTraversal(S, k));
    }

    // MAIN
    // ====

    public static void main(String[] args) throws IOException {
        // Checking command line arguments.
        if (args.length != 2) {
            throw new IllegalArgumentException("USAGE: num_partitions file_path");
        }

        // Read a path to a text file containing a set of points in Euclidean space, and an integer.
        String filename = args[0];
        Integer k = Integer.parseInt(args[1]);

        // Get the input set from given file.
        List<Vector> inputSet = readVectorsSequence(filename);

        // Run and measure the algorithms.
        runExactMPD(inputSet);
        System.out.println();

        runTwoApproxMPD(inputSet, k);
        System.out.println();

        runKCenterMPD(inputSet, k);
    }

    // AUX ALGORITHMS
    // ==============

    /** @return A list of k centers taken from S with farthest first traversal approach. */
    private static List<Vector> farthestFirstTraversal(List<Vector> S, Integer k) {
        @SuppressWarnings("UnnecessaryLocalVariable")
        List<Vector> points = S;                   // rename
        List<Vector> centers = new ArrayList<>();  // centers

        Random generator = new Random(SEED);  // the random generator

        /*
        Note: in the pseudo-code of this algorithm, selected centers are removed from the point set (the `points`
        collection in our case) but we have decided to do not remove them for the following reasons:
        - k is usually very small, i.e., in practise centers removal do not improve significantly the performance
        - the `remove(obj)` method can reduce performances if the object need to be searched
        - the `remove(index)` methods needs index management that can lead to errors and maintenance problems
        */

        final int random = generator.nextInt(points.size());  // random first center
        Vector first = points.get(random);
        centers.add(first);  // add to solution

        while (centers.size() < k) {
            Vector p = maximizeDistanceFromCenters(centers, points);
            centers.add(p);
        }

        return centers;
    }

    /** @return A point from S that maximizes the `distance` function among all centers in C. */
    private static Vector maximizeDistanceFromCenters(List<Vector> C, List<Vector> S) {
        Vector r = null;  // farthest point from centers the nearest center
        double max_d = Double.MIN_VALUE;

        for (Vector p : S) {
            double d = distance(p, C).first();

            if (d > max_d) {
                max_d = d;
                r = p;
            }
        }

        return r;
    }

    // DISTANCE AUX FUNCTIONS
    // ======================

    /** Generic pair class. */
    private static class Pair<A, B> {
        private final A first;
        private final B second;

        public Pair(A first, B second) {
            super();
            this.first = first;
            this.second = second;
        }

        public A first() { return first; }
        public B second() { return second; }

        public String toString() {
            return "(" + first + ", " + second + ")";
        }
    }

    /** @return The distance between p and q. */
    public static Double distance(Vector p, Vector q)  {
        return Vectors.sqdist(p, q);
    }

    /** @return A pair where the first value is the minimum distance between p and q in S, and the second pair
    // is itself a pair with the two p and q. */
    private static Pair<Double, Pair<Vector, Vector>> distance(Vector p, List<Vector> S) {
        double d = Double.MAX_VALUE;
        Vector r = null;

        for (Vector q : S) {
            d =Math.min(distance(p, q), d);
            r = q;
        }

        return new Pair<>(d, new Pair<>(p, r));
    }

    // AUX FUNCTIONS
    // =============

    // Run the exactMPD algorithm, measure the running time and pretty print results.
    private static void runExactMPD(List<Vector> S) {
        long start = System.currentTimeMillis();
        Double d = exactMPD(S);
        long end = System.currentTimeMillis();

        prettyPrint("EXACT ALGORITHM", d, end - start, OptionalInt.empty());
    }

    // Run the twoApproxMPD algorithm, measure the running time and pretty print results.
    private static void runTwoApproxMPD(List<Vector> S, Integer k) {
        long start = System.currentTimeMillis();
        Double d = twoApproxMPD(S, k);
        long end = System.currentTimeMillis();

        prettyPrint("2-APPROXIMATION ALGORITHM", d, end - start, OptionalInt.of(k));
    }

    // Run the kCenterMPD algorithm, measure the running time and pretty print results.
    private static void runKCenterMPD(List<Vector> S, Integer k) {
        long start = System.currentTimeMillis();
        Double d = kCenterMPD(S, k);
        long end = System.currentTimeMillis();

        prettyPrint("k-CENTER-BASED ALGORITHM", d, end - start, OptionalInt.of(k));
    }

    // Pretty prints result with the right format.
    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    private static void prettyPrint(String name, Double distance, Long runningTime, OptionalInt k) {
        System.out.println(name.toUpperCase());
        if (k.isPresent()) System.out.println("k = " + k.getAsInt());
        System.out.println("Max distance = " + distance);
        System.out.println("Running time = " + runningTime);
    }

    // Convert a comma separated string to a vector of double.
    private static Vector stringToVector(String str) {
        double[] data = Arrays.stream(str.split(","))
                .map(Double::parseDouble)
                .mapToDouble(Double::doubleValue)
                .toArray();

        return Vectors.dense(data);
    }

    // Convert a sequence of comma separated strings in a list of vector.
    private static ArrayList<Vector> readVectorsSequence(String filename) throws IOException {
        if (Files.isDirectory(Paths.get(filename))) {
            throw new IllegalArgumentException("readVectorsSeq is meant to read a single file.");
        }

        return Files.lines(Paths.get(filename))
                .map(G49HW2::stringToVector)
                .collect(Collectors.toCollection(ArrayList::new));
    }
}

package it.unipd.bdc.ay2019.group49.hw;

import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

public class G49HW2 {

    /**
     * Compute the exact solution of maximum pairwise problem on the given input set `S`.
     *
     * @param S A set of input points.
     * @return The exact maximum distance between two points in `S`.
     */
    public static Double exactMPD(List<Vector> S) {
        return 1d;
        // throw new UnsupportedOperationException("This method is not implemented yet.");
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
        return 1d;
        // throw new UnsupportedOperationException("This method is not implemented yet.");
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
        return 1d;
        // throw new UnsupportedOperationException("This method is not implemented yet.");
    }

    public static void main(String[] args) throws IOException {
        // Checking command line arguments.
        if (args.length != 2) {
            throw new IllegalArgumentException("USAGE: num_partitions file_path");
        }

        // Read number of partitions and input dataset.
        Integer k = Integer.parseInt(args[0]);
        String filename = args[1];

        // Get the input set from given file.
        List<Vector> inputSet = readVectorsSequence(filename);

        // Run and measure the algorithms.
        runExactMPD(inputSet);
        System.out.println();

        runTwoApproxMPD(inputSet, k);
        System.out.println();

        runKCenterMPD(inputSet, k);
        System.out.println();
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

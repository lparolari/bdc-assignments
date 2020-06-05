package it.unipd.bdc.ay2019.group49.hw;

import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

import java.io.IOException;
import java.util.*;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class G49HW3 {

    // Group 49: Luca Parolari, Giulio Piva
    // Homework 2, Big Data Computing
    // Assigned: 25/05/20, Deadline: 17/06/20

    // See [http://www.dei.unipd.it/~capri/BDC/homeworks.htm](http://www.dei.unipd.it/~capri/BDC/homeworks.htm)
    // and [http://www.dei.unipd.it/~capri/BDC/homework3.htm](http://www.dei.unipd.it/~capri/BDC/homework3.htm).

    public static final long SEED = 1236601; // my university id
    public static final SparkConf SPARK_CONF = new SparkConf(true)
            .setAppName("Homework3")
            .setMaster("local[*]");  // TODO: check if this is correct!

    private static final JavaSparkContext sc = buildContext(SPARK_CONF);

    public static void main(String[] args) throws IOException {
        // Step 1: SparkContext is initialized as a global variable.

        // Step 2: Parse arguments, load dataset, adn print information.

        // Parse arguments.
        String datasetPath = args[0];
        int L = Integer.parseInt(args[1]);
        int k = Integer.parseInt(args[2]);

        // Load the dataset and get running time.
        long start = System.currentTimeMillis();
        final JavaRDD<Vector> inputPoints = sc
                .textFile(datasetPath)
                .map(G49HW3::stringToVector)
                .repartition(L)
                .cache();  // materialize the rdd
        long end = System.currentTimeMillis();
        long runtime = end - start;

        // Print information.
        System.out.println("Number of points = " + inputPoints.count());
        System.out.println("k = " + k);
        System.out.println("L = " + L);
        System.out.println("Initialization time = " + runtime);
        System.out.println();

        // Step 3: run the map reduce algorithm and print runtime of round 1 and round 2 (done in the function).
        List<Vector> solution = runMapReduce(inputPoints, k, L);
        System.out.println();

        // Step 4: determine the average distance between points in solution.
        System.out.println("Average distance = " + measure(solution));
    }

    /**
     * Partition P into L subsets and extract k points from each subset using the Farthest-First Traversal algorithm.
     * Compute the final solution by running the 2-approximate sequential algorithm on the coreset of L*k points
     * extracted from the L subsets.
     *
     * Print, as a side effect, running time of round 1 and round 2.
     *
     * @param pointsRDD The points, already partitioned in L subset.
     * @param k The number of centers to select.
     * @param L The number of partitions for `pointsRDD`.
     * @return A list of k solution points computed by the sequential 2-approximation algorithm
     * for diversity maximization.
     */
    public static List<Vector> runMapReduce(final JavaRDD<Vector> pointsRDD, final int k, final int L) {
        long start, end, runtime;

        // .repartition(L) not needed as it hal already been done.

        // Round 1
        start = System.currentTimeMillis();
        JavaRDD<Vector> centersSubset = pointsRDD
                // Extracts k points from each partition using the Farthest-First Traversal algorithm.
                .glom()
                .mapPartitions((points) -> {
                    List<Vector> S = points.next();
                    List<Vector> centers = farthestFirstTraversal(S, k);
                    return centers.iterator();
                });
        long c = centersSubset.count();  // materialize the RDD
        end = System.currentTimeMillis();
        runtime = end - start;

        if (c != k * L)
            throw new AssertionError("Coreset size is not k*L");

        System.out.println("Runtime of Round 1 = " + runtime);

        // Round 2
        start = System.currentTimeMillis();
        List<Vector> coreset = centersSubset.collect();    // collects the L*k points
        List<Vector> centers = runSequential(coreset, k);  // execute 2-approx algorithm
        end = System.currentTimeMillis();
        runtime = end - start;

        System.out.println("Runtime of Round 2 = " + runtime);

        return centers;
    }

    /**
     * Computes the average distance between all pairs of points.
     * @param points A list of vector representing points.
     * @return The average distance between points in `points`.
     */
    public static double measure(final List<Vector> points) {
        double sum = 0;
        for (int i = 0; i < points.size(); i++) {
            for (int j = i + 1; j < points.size(); j++)
                sum += Math.sqrt(Vectors.sqdist(points.get(i), points.get(j)));
        }
        int size = points.size() * (points.size() - 1) / 2;
        return sum / size;
    }

    /**
     * Run the sequential 2-approximation algorithm for diversity maximization.
     * @param points The points dataset.
     * @param k The number of points to select.
     * @return A list of k selected points.
     */
    public static List<Vector> runSequential(final List<Vector> points, int k) {

        final int n = points.size();
        if (k >= n) {
            return points;
        }

        ArrayList<Vector> result = new ArrayList<>(k);
        boolean[] candidates = new boolean[n];
        Arrays.fill(candidates, true);
        for (int iter = 0; iter < k / 2; iter++) {
            // Find the maximum distance pair among the candidates
            double maxDist = 0;
            int maxI = 0;
            int maxJ = 0;
            for (int i = 0; i < n; i++) {
                if (candidates[i]) {
                    for (int j = i + 1; j < n; j++) {
                        if (candidates[j]) {
                            // Use squared euclidean distance to avoid an sqrt computation!
                            double d = Vectors.sqdist(points.get(i), points.get(j));
                            if (d > maxDist) {
                                maxDist = d;
                                maxI = i;
                                maxJ = j;
                            }
                        }
                    }
                }
            }
            // Add the points maximizing the distance to the solution
            result.add(points.get(maxI));
            result.add(points.get(maxJ));
            // Remove them from the set of candidates
            candidates[maxI] = false;
            candidates[maxJ] = false;
        }
        // Add an arbitrary point to the solution, if k is odd.
        if (k % 2 != 0) {
            for (int i = 0; i < n; i++) {
                if (candidates[i]) {
                    result.add(points.get(i));
                    break;
                }
            }
        }
        if (result.size() != k) {
            throw new IllegalStateException("Result of the wrong size");
        }
        return result;

    } // END runSequential

    /**
     * Farthest first traversal algorithm.
     * @return A list of k centers taken from S with farthest first traversal approach.
     */
    private static List<Vector> farthestFirstTraversal(final List<Vector> S, final Integer k) {
        @SuppressWarnings("UnnecessaryLocalVariable")
        List<Vector> points = S;                   // rename
        List<Vector> centers = new ArrayList<>();  // centers

        Random generator = new Random(SEED); // the random generator

        /*
        Note: in the pseudo-code of this algorithm, selected centers are removed from the point set (the `points`
        collection in our case) but we have decided to do not remove them for the following reasons:
        - k is usually very small, i.e., in practise centers removal do not improve significantly the performance
        - the `remove(obj)` method can reduce performances if the object need to be searched
        - the `remove(index)` method needs index management that can lead to errors and maintenance problems
        */

        final int random = generator.nextInt(points.size()); // random first center
        Vector first = points.get(random);
        centers.add(first); // add to solution
        List<Pair<Vector, Pair<Vector, Double>>> distances = new LinkedList<>();
        // initialize all point to have the first center as nearest center
        Vector previousCenter = initializeDistances(first, S, distances); // initialize all point-distance pairs and return the second center
        centers.add(previousCenter);
        while (centers.size() < k) {
            Vector nextcenter = maximizeDistanceFromCenters(previousCenter, distances);
            centers.add(nextcenter);
            previousCenter = nextcenter;
        }

        return centers;
    }

    private static Vector initializeDistances(Vector firstCenter, List<Vector> S,
                                              List<Pair<Vector, Pair<Vector, Double>>> distances) {
        Vector nextCenter = null;
        double maxMinDistance = Double.MIN_VALUE;
        for (Vector p : S) {
            double dist = Vectors.sqdist(p, firstCenter);
            Pair<Vector, Double> centerDistance = new Pair<>(firstCenter, dist);
            Pair<Vector, Pair<Vector, Double>> pointCenterPair = new Pair<>(p, centerDistance);
            distances.add(pointCenterPair);
            if (dist > maxMinDistance) {
                maxMinDistance = dist;
                nextCenter = p;
            }
        }
        return nextCenter;
    }

    private static Vector maximizeDistanceFromCenters(Vector currentCenter,
                                                      List<Pair<Vector, Pair<Vector, Double>>> distances) {
        double maxMinDistance = Double.MIN_VALUE;
        Vector nextCenter = null;
        for (Pair<Vector, Pair<Vector, Double>> point : distances) {
            double distanceFromCurrentCenter = Vectors.sqdist(point.first(), currentCenter);
            double currentDistance = point.second().second();
            if (distanceFromCurrentCenter < currentDistance) { // update the current point's nearest center and distance, if smaller
                point.second().setFirst(currentCenter);
                point.second().setSecond(distanceFromCurrentCenter);
            }
            if (point.second().second() > maxMinDistance) { //update next center according to maximum-minimum distance
                maxMinDistance = point.second().second();
                nextCenter = point.first();
            }
        }

        return nextCenter;
    }

    /** Build a JavaSparkContext object with given configurations. */
    private static JavaSparkContext buildContext(SparkConf conf) {
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN");

        return sc;
    }

    private static class Pair<A, B> {
        private A first;
        private B second;

        public Pair(A first, B second) {
            super();
            this.first = first;
            this.second = second;
        }

        public A first() {
            return first;
        }

        public B second() {
            return second;
        }

        public void setFirst(A first) {
            this.first = first;
        }

        public void setSecond(B second) {
            this.second = second;
        }

        public String toString() {
            return "(" + first + ", " + second + ")";
        }
    }

    private static Vector stringToVector(String str) {
        double[] data = Arrays.stream(str.split(",")).map(Double::parseDouble).mapToDouble(Double::doubleValue)
                .toArray();

        return Vectors.dense(data);
    }
}

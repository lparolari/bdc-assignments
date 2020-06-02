
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class G49HW3 {

    public static final long SEED = 1236601; // my university id

    public static void main(String[] args) throws IOException {
        SparkConf conf = new SparkConf(true).setAppName("Homework1").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN");

        String datasetPath = args[0];
        int L = Integer.parseInt(args[1]);
        int k = Integer.parseInt(args[2]);

        JavaRDD<Vector> dataset = sc.textFile(datasetPath).map(G49HW3::stringToVector).repartition(L).cache();
        System.out.println("average distance: " + measure(runMapReduce(k, L, datasetPath, sc)));
    }

    public static double measure(List<Vector> points) {
        double sum = 0;
        for (int i = 0; i < points.size(); i++) {
            for (int j = i + 1; j < points.size(); j++)
                sum += Math.sqrt(Vectors.sqdist(points.get(i), points.get(j)));
        }
        int size = points.size() * (points.size() - 1) / 2;
        return sum / size;
    }

    public static List<Vector> runMapReduce(int k, int L, String datasetPath, JavaSparkContext sc) {
        JavaRDD<Vector> dataset = sc.textFile(datasetPath).map(G49HW3::stringToVector).repartition(L).cache();
        List<Vector> res = dataset.glom().mapPartitions((points) -> {
            List<Vector> S = points.next();
            List<Vector> centers = farthestFirstTraversal(S, k);
            return centers.iterator();
        }).collect();
        return runSequential(res, k);

    }

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

    /** @return A list of k centers taken from S with farthest first traversal approach. */
    private static List<Vector> farthestFirstTraversal(List<Vector> S, Integer k) {
        @SuppressWarnings("UnnecessaryLocalVariable")
        List<Vector> points = S; // rename
        List<Vector> centers = new ArrayList<>(); // centers

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
            Pair<Vector, Double> centerDistance = new Pair(firstCenter, dist);
            Pair<Vector, Pair<Vector, Double>> pointCenterPair = new Pair(p, centerDistance);
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

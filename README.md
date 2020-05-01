# Big Data Computing Assignments

Implementation of some famous big data algorithm for the Big Data Computing course.

## Example

From command line with gradle
```
./gradlew <TASK> --args <ARGS>

# For example
./gradlew runExample --args "4 \"src/main/resources/examples/dataset.txt\""
```

From IntelliJ
- add a profile *build & run*
- set the desired class to execute
- add VM the following option `-Dspark.master="local[*]"`
- set the program arguments
- run

Note: the spark master is set programmatically, so there is no
needs for `-Dspark.master="local[*]"`.

## :heavy_check_mark: Implemented Algorithms

**HW1: Class Count**

- Deterministic Partition Size
- Spark Partition Size

Run the algorithm
```
# Small example dataset
./gradlew runClassCount --args "4 \"src/main/resources/hw1/example.txt\""

# 10000-entry dataset
./gradlew runClassCount --args "4 \"src/main/resources/hw1/input_10000.txt\""
```

**HW2: Max Pairwise Distance**

- Exact solution
- 2-approximation by taking k random points
- Exact solution of a subset of C of S, where C are k centers retruned by Farthest-First Traversal

```
./gradlew runMaxPairwiseDistance --args "\"src/main/resources/hw2/aircraft-mainland.csv\" 4"
./gradlew runMaxPairwiseDistance --args "\"src/main/resources/hw2/uber-small.csv\" 4"
./gradlew runMaxPairwiseDistance --args "\"src/main/resources/hw2/uber-medium.csv\" 4"
./gradlew runMaxPairwiseDistance --args "\"src/main/resources/hw2/uber-large.csv\" 4"
```

## :busts_in_silhouette: Authors

**Luca Parolari**

- Github: @lparolari

**Giulio Piva**

## :memo: License

The project is MIT licensed. See [LICENSE](LICENSE) file.

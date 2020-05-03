# Big Data Computing Assignments

Implementation of some famous big data algorithm for the Big Data Computing course.

## 🚀 Getting Started

Download the repository
```
git clone git@github.com:lparolari/bdc-assignments.git
# or, with https
# git clone https://github.com/lparolari/bdc-assignments.git

cd bcd-assignments
```

Run our algorithms (see next section) with gradle from the
command line
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

## ✔️ Implemented Algorithms

### HW1: Class Count

- Deterministic Partition Size
- Spark Partition Size

Run the algorithm
```
# Small example dataset
./gradlew runClassCount --args "4 \"src/main/resources/hw1/example.txt\""

# 10000-entry dataset
./gradlew runClassCount --args "4 \"src/main/resources/hw1/input_10000.txt\""
```

### HW2: Max Pairwise Distance

- Exact solution
- 2-approximation by taking k random points
- Exact solution of a subset of C of S, where C are k centers retruned by Farthest-First Traversal

```
./gradlew runMaxPairwiseDistance --args "\"src/main/resources/hw2/aircraft-mainland.csv\" 4"
./gradlew runMaxPairwiseDistance --args "\"src/main/resources/hw2/uber-small.csv\" 4"
./gradlew runMaxPairwiseDistance --args "\"src/main/resources/hw2/uber-medium.csv\" 4"
./gradlew runMaxPairwiseDistance --args "\"src/main/resources/hw2/uber-large.csv\" 4"
```

**Running time**

The following table summarizes the running time for the algorithms benchmarked over
different dataset and with some k value. Times are expressed in millisecond. This runs
refer only to correct results. Data are collected in "one shot".

Algorithm | Aircraft Mainland | Uber (SM) | Uber (MD) | Uber (LG) | Notes
--- | --- | --- | --- | --- | ---
Exact algorithm | 2468 | 136 | 2078582 |  | TODO: Uber Large
2-approx algorithm (k = 4) |  |  |  |  | TODO: all
k-center based algorithm (k = 4) | 9 | 2 | 78 |  |


Algorithm (variant) | Aircraft Mainland | Uber (SM) | Uber (MD) | Uber (LG) | Notes
--- | --- | --- | --- | --- | ---
k-center based algorithm (k = 700) |  |  | 1120479 |  | Note: k = 700 =~ sqrt(N), where N is the size of Uber Medium (=~ 500k entries) |

## 👥 Authors

**Luca Parolari**

- Github: @lparolari

**Giulio Piva**

## 🙏 Credits

Thanks to our big data course tutors.
Take a look at their [course web page](http://www.dei.unipd.it/~capri/BDC/).

## 📝 License

The project is MIT licensed. See [LICENSE](LICENSE) file.

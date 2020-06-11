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

*Comparative with K = 4 (only for 2-approx and k-center)*

Dataset | Exact | 2-approx | kCenter
--- | --- | --- | ---
Aircraft Mainland | 2468 | 190 | 197
Uber Small | 136 | 156 | 176 |
Uber Medium | 2078582 | 196 | 504 |
Uber Large | n/d | 675 | 822 |

*Comparative with K = 128 (only for 2-approx and k-center)*

Dataset | Exact | 2-approx | kCenter
--- | --- | --- | ---
Aircraft Mainland | 2468 | 264 | 1993
Uber Small | 136 | 154 | 266 |
Uber Medium | 2078582 | 1743 | 52554 |
Uber Large | n/d | 2992 | 169519 |

*Full benchmark list*

Algorithm | Dataset | K | Time (ms)
--- | --- | --- | ---
exact | Aircraft Mainland | - | 2468
exact | Uber Small | - | 136
exact | Uber Medium | - | 2078582
exact | Uber Large | - | n/d
2-approx | Aircraft Mainland | 4 | 190
2-approx | Aircraft Mainland | 8 | 204
2-approx | Aircraft Mainland | 32 | 485
2-approx | Aircraft Mainland | 128 | 264
2-approx | Uber Small | 4 | 156
2-approx | Uber Small | 8 | 202
2-approx | Uber Small | 32 | 168
2-approx | Uber Small | 128 | 154
2-approx | Uber Medium | 4 | 196
2-approx | Uber Medium | 8 | 387
2-approx | Uber Medium | 32 | 473
2-approx | Uber Medium | 128 | 1743
2-approx | Uber Large | 4 | 675
2-approx | Uber Large | 8 | 462
2-approx | Uber Large | 32 | 963
2-approx | Uber Large | 128 | 2992
kCenter | Aircraft Mainland | 4 | 197
kCenter | Aircraft Mainland | 8 | 227
kCenter | Aircraft Mainland | 32 | 639
kCenter | Aircraft Mainland | 128 | 1993
kCenter | Uber Small | 4 | 176
kCenter | Uber Small | 8 | 148
kCenter | Uber Small | 32 | 191
kCenter | Uber Small | 128 | 266
kCenter | Uber Medium | 4 | 504
kCenter | Uber Medium | 8 | 947
kCenter | Uber Medium | 32 | 5395
kCenter | Uber Medium | 128 | 52554
kCenter | Uber Large | 4 | 882
kCenter | Uber Large | 8 | 1431
kCenter | Uber Large | 32 | 12574
kCenter | Uber Large | 128 | 169519

**Run the benchmarks**

If you want to run the benchmarks yourself you can look at `hw2run.sh`, `hw2scale.sh`
and `hw2bench.sh` scripts that we made available in order to simplify this task.

Please note that benchmarking was not the aim of this exercise. We took running times
*one shot*, and we decided to leave benchmark results out of the source control.

### HW3: Diversity Maximization

**Execute locally**

```shell script
./gradlew runDiversityMaximization --args "\"src/main/resources/hw3/uber-small.csv\" 4 4"
```

**Execute on cloud**

- Step 1: Build the shadow jar with
```shell script
./gradlew shadowJar
```
We suggest deleting the *build* dir before re-build the shadow jar.
Please be sure to use Java 8 when compiling. You can check and fix versions with
```shell script
java -version
javac -version

sudo update-alternatives --config java
sudo update-alternatives --config javac
```

- Step 2: Copy the fat jar to a machine in the unipd network (skip this step if you are under the unipd network).
```shell script
scp build/libs/bdc-assignments-all.jar torre.studenti.math.unipd.it:dev/bdc

# or, if you don't have configured the connection with key pairs (password required)
scp build/libs/bdc-assignments-all.jar username@torre.studenti.math.unipd.it:dev/bdc
```

- Step 3: Copy the jar to the cluster
```shell script
scp dev/bdc/bdc-assignments-all.jar group49@147.162.226.106:.
```

- Step 4: Run the job with Spark
```shell script
spark-submit \
  --num-executors 4 \
  --class it.unipd.bdc.ay2019.group49.hw.G49HW3\
  bdc-assignments-all.jar\
  /data/BDC1920/uber-small.csv 4 4
```

## 👥 Authors

**Luca Parolari**

- Github: @lparolari

**Giulio Piva**

## 🙏 Credits

Thanks to our big data course tutors.
Take a look at their [course web page](http://www.dei.unipd.it/~capri/BDC/).

## 📝 License

The project is MIT licensed. See [LICENSE](LICENSE) file.

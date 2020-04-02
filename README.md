# Big Data Computing Assignments

TODO 

## Example

From command line with gradle
```
./gradlew <TASK> --args <ARGS>

# For example
./gradlew example --args "4 \"src/main/resources/examples/dataset.txt\""
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

**Class Count**

- Deterministic Partition Size
- Spark Partition Size
  
Run the algorithm
```
./gradlew runClassCount --args "4 \"src/main/resources/examples/input_10000.txt\""
```

## :busts_in_silhouette: Authors

**Luca Parolari**

- Github: @lparolari

**Giulio Piva**

## :memo: License

The project is MIT licensed. See [LICENSE](LICENSE) file.
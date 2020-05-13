#!/bin/bash

# Execute the algorithms from hw2 with given parameters.

if [ $# -lt 2 ]; then
  echo "Usage: hw2run.sh DATASET K [ALG]"
  echo "  where"
  echo "    * K is an integer"
  echo "    * DATASET is a path to a dataset"
  echo "    * ALG is one of {exact, 2approx, kCenter}, optional"
  echo ""
  echo "Run the given algorithm (or all if not specified) on given dataset"
  echo "with given K."
fi

dataset=${1}
k=${2}
alg=${3}

./gradlew runMaxPairwiseDistance --args "${dataset} ${k} ${alg}"

#!/bin/bash

# Execute the the algorithms for hw2 (or the algorithm if ALG specified)
# over the dataset DATASET with k = {4, 8, 32, 128}.

if [ $# -lt 1 ]; then
  echo "Usage: hw2scale.sh DATASET [ALG]"
  echo "  where"
  echo "    * DATASET is a path to a dataset"
  echo "    * ALG is one of {exact, 2approx, kCenter}, optional"
  echo ""
  echo "Run the algorithm on given dataset with some values for the parameter K as"
  echo "background jobs and wait them to finish. K is one of 4, 8, 32 or 128."
  echo ""
  echo "Note: the script expects a variable OUT_DIR, representing the path for out logs."
fi

dataset=${1}
alg=${2}

for k in 4 8 32 128; do
  d=$(date +%Y%m%j-%H%M%S-%N)
  out="${OUT_DIR}/${d}.txt"
  ./hw2run.sh ${dataset} ${k} ${alg} >> ${out} &
done
wait

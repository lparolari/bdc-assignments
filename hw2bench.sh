#!/bin/bash

# NOTE: be sure that you have enough RAM available or your computer will freeze.

export OUT_DIR="benchmark/exact/am"; mkdir -p ${OUT_DIR}; ./hw2scale.sh "src/main/resources/hw2/aircraft-mainland.csv" "exact"
export OUT_DIR="benchmark/exact/us"; mkdir -p ${OUT_DIR}; ./hw2scale.sh "src/main/resources/hw2/uber-small.csv" "exact"
export OUT_DIR="benchmark/exact/um"; mkdir -p ${OUT_DIR}; ./hw2scale.sh "src/main/resources/hw2/uber-medium.csv" "exact"
export OUT_DIR="benchmark/exact/ul"; mkdir -p ${OUT_DIR}; ./hw2scale.sh "src/main/resources/hw2/uber-large.csv" "exact"

export OUT_DIR="benchmark/2approx/am"; mkdir -p ${OUT_DIR}; ./hw2scale.sh "src/main/resources/hw2/aircraft-mainland.csv" "2approx"
export OUT_DIR="benchmark/2approx/us"; mkdir -p ${OUT_DIR}; ./hw2scale.sh "src/main/resources/hw2/uber-small.csv" "2approx"
export OUT_DIR="benchmark/2approx/um"; mkdir -p ${OUT_DIR}; ./hw2scale.sh "src/main/resources/hw2/uber-medium.csv" "2approx"
export OUT_DIR="benchmark/2approx/ul"; mkdir -p ${OUT_DIR}; ./hw2scale.sh "src/main/resources/hw2/uber-large.csv" "2approx"

export OUT_DIR="benchmark/kCenter/am"; mkdir -p ${OUT_DIR}; ./hw2scale.sh "src/main/resources/hw2/aircraft-mainland.csv" "kCenter"
export OUT_DIR="benchmark/kCenter/us"; mkdir -p ${OUT_DIR}; ./hw2scale.sh "src/main/resources/hw2/uber-small.csv" "kCenter"
export OUT_DIR="benchmark/kCenter/um"; mkdir -p ${OUT_DIR}; ./hw2scale.sh "src/main/resources/hw2/uber-medium.csv" "kCenter"
export OUT_DIR="benchmark/kCenter/ul"; mkdir -p ${OUT_DIR}; ./hw2scale.sh "src/main/resources/hw2/uber-large.csv" "kCenter"

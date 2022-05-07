#!/bin/bash
# build IR files for OmniOperatorJit
# Copyright (c) Huawei Technologies Co., Ltd. 2021-2022. All rights reserved.
set -e

# Get the source file input array
OLD_IFS="$IFS"
IFS=";"
array=($3)
IFS="$OLD_IFS"

echo "build IR start"

# build IR files concurrently
for var in ${array[@]}
do
  IR="$1 $2 $var $4 $5"
  echo $IR
  $IR &
done
wait
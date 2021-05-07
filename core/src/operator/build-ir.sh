#!/bin/bash

echo $PWD

clang++-12 -S -O3 -emit-llvm -fno-discard-value-names -I/usr/include/llvm-12/ -I/usr/include/llvm-c-12/ $PWD/op_template.cpp $PWD/aggregator/aggregator.cpp $PWD/sort/sort.h $PWD/sort/sort.cpp $PWD/aggregator/hash_groupby.cpp $PWD/../memory/memory_pool.cpp $PWD/../jni/sort_api.cpp $PWD/../jni/sort_api.h
# clang++-12 -S -O2 -emit-llvm -fno-discard-value-names ../aggregator.cpp
#clang++-12 -S  -emit-llvm -mllvm,print-after-all -fno-discard-value-names ../op_template.cpp ../op_template.h ../aggregator.cpp ../aggregator.h ../sort.h ../sort.cpp ../hash_groupby.cpp ../hash_groupby.h ../test.cpp

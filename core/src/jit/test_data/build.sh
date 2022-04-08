#!/bin/bash
# data gen for test data
# Copyright (c) Huawei Technologies Co., Ltd. 2021-2022. All rights reserved.
clang++-12 -S -O3 -emit-llvm -fno-discard-value-names *.cpp

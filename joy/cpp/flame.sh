#!/bin/bash

perf script -i perf.data &> perf.unfold
/usr/code/flameGraph/FlameGraph/stackcollapse-perf.pl perf.unfold &> perf.folded
/usr/code/flameGraph/FlameGraph/flamegraph.pl perf.folded > perf.svg

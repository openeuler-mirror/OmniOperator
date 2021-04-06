#clang++-12 -g hammer.h hammer.cpp param.h optimizer.h hammer_test.cpp perf_test.cpp SimpleOptimizer.h SimpleOptimizer.cpp `llvm-config-12 --cxxflags --ldflags --libs` -lstdc++ -lgtest -lpthread -o hammer_test
g++ -g `ls *.cpp *.h` `llvm-config --cxxflags --ldflags --libs` -lstdc++ -lgtest -lpthread -o hammer_test
#g++ -g hammer.h hammer.cpp param.h optimizer.h hammer_test.cpp perf_test.cpp SimpleOptimizer.h SimpleOptimizer.cpp -lz -lgtest -lpthread `/opt/kkrazy/llvm-project/build/bin/llvm-config --cxxflags --ldflags --libs` -o hammer_test
#g++ -g MultiModuleHardening.cpp `llvm-config --cxxflags --ldflags --libs` -o main
# clang++ -O3 op_template.c `llvm-config --cxxflags --ldflags --libs` -o op
# clang-12 -S -O3 -emit-llvm -fno-discard-value-names src/operator/op_template.cpp src/operator/op_template.h src/operator/aggregator.cpp src/operator/aggregator.h src/operator/hash_groupby.cpp src/operator/hash_group.h

#clang++-12 -S -O3 -emit-llvm -fno-discard-value-names src/operator/op_template.cpp src/operator/op_template.h src/operator/aggregator.cpp src/operator/aggregator.h src/operator/hash_groupby.cpp src/operator/hash_groupby.h src/util/type_infer.h src/util/type_infer.h src/data/table.h src/data/table.cpp

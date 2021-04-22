#!/bin/bash  

GREEN='\033[0;32m'
NC='\033[0m'
libFolder="/opt/lib/"
irFolder="/opt/lib/ir"

if [ ! -d "$libFolder" ]; then
	mkdir -p "$libFolder"
fi
 
if [ ! -d "$irFolder" ]; then
        mkdir -p "$irFolder" 
fi

cd src/operator/ir
sh ./build-ir.sh
cp *.ll "$irFolder"
echo "${GREEN}==================================BUILD IR FINISHED=================================="
echo "${NC}"

cd ../../../
if [ $1 = 'debug' ] && [ $2 = 'low' ];then
    clang++-12 -g -ggdb -O2 -D DEBUG_LEVEL_LOW -fPIC -I"$JAVA_HOME/include" -I"$JAVA_HOME/include/linux/"  src/jni/*.cpp  src/operator/*.cpp src/data/*.cpp src/util/*.cpp src/memory_pool/*.cpp src/harden/Hammer.cpp src/harden/HammerConfig.cpp src/harden/HardenOptimizer.cpp `llvm-config-12 --cxxflags --ldflags --libs` -lpthread -ldl -lgtest -lstdc++ -shared -o libjoy.so
elif [ $1 = 'debug' ] && [ $2 = 'high' ];then
    clang++-12 -g -ggdb -O2 -D DEBUG_LEVEL_HIGH -fPIC -I"$JAVA_HOME/include" -I"$JAVA_HOME/include/linux/"  src/jni/*.cpp  src/operator/*.cpp src/data/*.cpp src/util/*.cpp src/memory_pool/*.cpp src/harden/Hammer.cpp src/harden/HammerConfig.cpp src/harden/HardenOptimizer.cpp -ljemalloc `llvm-config-12 --cxxflags --ldflags --libs` -lpthread -ldl -lgtest -lstdc++ -shared -o libjoy.so
elif [ $1 = 'release' ];then
    clang++-12 -g -dwarf-3 -O2 -fPIC -I"$JAVA_HOME/include" -I"$JAVA_HOME/include/linux/"  src/jni/*.cpp  src/operator/*.cpp src/data/*.cpp src/util/*.cpp src/memory_pool/*.cpp src/harden/Hammer.cpp src/harden/HammerConfig.cpp src/harden/HardenOptimizer.cpp -ljemalloc `llvm-config-12 --cxxflags --ldflags --libs` -lpthread -ldl -lgtest -lstdc++ -shared -o libjoy.so
    cp libjoy.so /opt/lib/
elif [ $1 = 'test' ];then
    clang++-12 -g -O2 -dwarf-3 -fPIC -I"$JAVA_HOME/include" -I"$JAVA_HOME/include/linux/"  -I"./tests/" src/jni/api.cpp src/jni/sort_api.cpp  src/operator/*.cpp src/data/*.cpp src/util/*.cpp src/memory_pool/*.cpp src/harden/Hammer.cpp src/harden/HammerConfig.cpp src/harden/HardenOptimizer.cpp tests/hash_groupby_test.cpp tests/sort_test.cpp ./main.cpp `llvm-config-12 --cxxflags --ldflags --libs` -ljemalloc -lpthread -ldl -lgtest -lstdc++ -o main_test
    # ./main_test
else echo "No such building type."
fi
echo "${GREEN}==================================COMPILE FINISHED=================================="
echo "${NC}"

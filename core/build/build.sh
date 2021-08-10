#!/bin/bash

echo "enter" $(dirname $(readlink -f $0))
cd $(dirname $(readlink -f $0))
rm -rf `ls | grep -v "build.sh"`
if [ $# != 0 ] ; then
  if [ $1 = 'debug' ] && [ $2 = 'low' ];then
    echo "-- Enable low level debug"
    cmake ../ -DDEBUG_LEVEL_LOW=ON -DCMAKE_BUILD_TYPE=Debug
  elif [ $1 = 'debug' ] && [ $2 = 'high' ];then
    echo "-- Enable high level debug"
    cmake ../ -DDEBUG_LEVEL_HIGH=ON -DCMAKE_BUILD_TYPE=Debug
  elif [ $1 = 'debug' ] && [ $2 = 'op' ];then
    echo "-- Enable native operator debug"
    cmake ../ -DDEBUG_OPERATOR=ON  -DCMAKE_BUILD_TYPE=Debug
  elif [ $1 = 'release' ];then
    cmake ../  -DCMAKE_BUILD_TYPE=Release
  fi
else
  cmake ../ -DCMAKE_BUILD_TYPE=Release
fi
make clean
make -j4
make install
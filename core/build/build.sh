#!/bin/bash

rm -rf ../cmake-build-debug
find . -type f ! -name 'build.sh' -delete
if [ $# != 0 ] ; then
  if [ $1 = 'debug' ] && [ $2 = 'low' ];then
    echo "-- Enable low level debug"
    cmake ../ -DDEBUG_LEVEL_LOW=ON
  elif [ $1 = 'debug' ] && [ $2 = 'high' ];then
    echo "-- Enable high level debug"
    cmake ../ -DDEBUG_LEVEL_HIGH=ON
  elif [ $1 = 'debug' ] && [ $2 = 'op' ];then
    echo "-- Enable native operator debug"
    cmake ../ -DDEBUG_OPERATOR=ON
  elif [ $1 = 'release' ];then
    cmake ../
  fi
else
  cmake ../
fi
make clean
make -j4
make install
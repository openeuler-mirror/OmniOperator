#!/bin/bash

# clean environment
if [[ -z $OMNI_HOME ]]; then
  echo "OMNI_HOME is empty"
  OMNI_HOME=/opt
fi

lib_home=$OMNI_HOME/lib
[ ! -d "lib_home" ] && echo mkdir -p $lib_home

echo "lib_home = $lib_home, LD_LIBRARY_PATH = $LD_LIBRARY_PATH"

rm -rf libomni_runtime.so $lib_home/libomni_vector.so $lib_home/libsecurec.so $lib_home/externalfunctions.so $lib_home/ir $lib_home/jit_libs
echo "-- Enter" $(dirname $(readlink -f $0))
cd $(dirname $(readlink -f $0))
rm -rf `ls | grep -v "build.sh"`

print_gcc_lib() {
  gcc -print-search-dirs | sed '/^lib/b 1;d;:1;s,/[^/.][^/]*/\.\./,/,;t 1;s,:[^=]*=,:;,;s,;,;  ,g' | tr \; \\012
}
#append_options
append_options()
{
  if [ $# = 1 ]; then
    echo "-- Enable Debug"
  elif [ $2 = 'all' ]; then
    echo "-- Enable All Module Debug, Include: OPERATOR,VECTOR,LLVM"
    options="$options -DDEBUG_OPERATOR=ON -DDEBUG_VECTOR=ON -DDEBUG_LLVM=ON"
  else
    for i in $* ; do
        if [ $i != $1 ]; then
            if [ $i = 'op' ]; then
              echo "-- Enable Operator Debug"
              options="$options -DDEBUG_OPERATOR=ON"
            elif [ $i = 'vec' ]; then
              echo "-- Enable Vector Debug"
              options="$options -DDEBUG_VECTOR=ON"
            elif [ $i = 'llvm' ]; then
              echo "-- Enable LLVM Debug"
              options="$options -DDEBUG_LLVM=ON"
            elif [ $i = '--disable-jit' ]; then
              echo "-- Disable JIT"
              options="$options -DDISABLE_JIT=ON"
            fi
        fi
    done
  fi
}

# options
if [ $# != 0 ] ; then
  options=""
  if [ $1 = 'debug' ]; then
    echo "-- Enable Debug"
    options="$options -DCMAKE_BUILD_TYPE=Debug -DDEBUG=ON"
    append_options $*
  elif [ $1 = 'trace' ]; then
    echo "-- Enable Trace"
    options="$options -DCMAKE_BUILD_TYPE=Debug -DTRACE=ON"
    append_options $*
  elif [ $1 = 'release' ];then
    echo "-- Enable Release"
    options="$options -DCMAKE_BUILD_TYPE=Release"
    if [ $2 = '--disable-jit' ]; then
      echo "-- Disable JIT"
      options="$options -DDISABLE_JIT=ON"
    fi
  fi
  print_gcc_lib
  cmake ../ $options
else
  echo "-- Enable Release"
  print_gcc_lib
  cmake ../ -DCMAKE_BUILD_TYPE=Release
fi
make clean
make -j16
make install

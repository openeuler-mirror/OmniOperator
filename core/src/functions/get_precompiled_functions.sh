#!/bin/bash
set -e
# Convert bitcode file to a C++ unsigned char array
# Save length variable of type size_t with 'Size' suffix
arrname="precompiledBitcode"
# Extra null byte added on the end of the array (not included in len) to be used as a string.
null_terminate=1
# For size_t
echo "#include <stddef.h>"
echo
# Preceding extern declaration guarantees external linkage in C++
echo "extern const unsigned char ${arrname}[];";
echo "extern const size_t ${arrname}Size;"
echo
echo "const unsigned char ${arrname}[] = {"
xxd -i < $1
if [ ${null_terminate} = 1 ]; then
  echo ", 0x0"
fi
echo "};"
echo -n "const size_t ${arrname}Size = "
if [ ${null_terminate} = 1 ]; then
  echo "sizeof(${arrname}) - 1;"
else
  echo "sizeof(${arrname});"
fi
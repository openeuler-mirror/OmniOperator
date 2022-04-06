#!/bin/bash
set -e
# Convert bitcode file to a C++ unsigned char array
# Save length variable of type size_t with 'Size' suffix
FUNCTIONS_ARRAY="precompiledBitcode"
# Extra null byte added on the end of the array (not included in len) to be used as a string.
NULL_BYTES=1
# For size_t
echo "#include <stddef.h>"
echo
# Preceding extern declaration guarantees external linkage in C++
echo "extern const unsigned char ${FUNCTIONS_ARRAY}[];";
echo "extern const size_t ${FUNCTIONS_ARRAY}Size;"
echo
echo "const unsigned char ${FUNCTIONS_ARRAY}[] = {"
xxd -i < $1
if [ ${NULL_BYTES} = 1 ]; then
  echo ", 0x0"
fi
echo "};"
echo -n "const size_t ${FUNCTIONS_ARRAY}Size = "
if [ ${NULL_BYTES} = 1 ]; then
  echo "sizeof(${FUNCTIONS_ARRAY}) - 1;"
else
  echo "sizeof(${FUNCTIONS_ARRAY});"
fi
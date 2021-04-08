### how to build omvector
```shell script
#add link joy and jemalloc so ,then build
RUSTFLAGS="-L /usr/java/packages/lib" cargo build --release
#Config execute link joy and jemalloc so 
export LD_LIBRARY_PATH=/usr/java/packages/lib
```
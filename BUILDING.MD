#Preface
This document describes how to compile and deploy the openLooKeng (OLK) and OmniRuntime package for function and 
performance debugging in ***yellow zone***. The operating system uses ***ubuntu 18.04***.

#Preparing the Compilation Environment

##Configure Proxy

Add proxy with running instructions below or add it in file `/etc/profile`
```
export http_proxy=http://username:password@proxy.huawei.com:8080
export https_proxy=http://username:password@proxy.huawei.com:8080
```

##Install Dependencies
We need to install dependencies below before compiling OmniRuntime.

###Update apt sources
Step 1: add sources below to `/etc/apt/sources.list`
```
deb http://mirrors.aliyun.com/ubuntu/ bionic main restricted universe multiverse
deb http://mirrors.aliyun.com/ubuntu/ bionic-security main restricted universe multiverse
deb http://mirrors.aliyun.com/ubuntu/ bionic-updates main restricted universe multiverse
deb http://mirrors.aliyun.com/ubuntu/ bionic-proposed main restricted universe multiverse
deb http://mirrors.aliyun.com/ubuntu/ bionic-backports main restricted universe multiverse
deb-src http://mirrors.aliyun.com/ubuntu/ bionic main restricted universe multiverse
deb-src http://mirrors.aliyun.com/ubuntu/ bionic-security main restricted universe multiverse
deb-src http://mirrors.aliyun.com/ubuntu/ bionic-updates main restricted universe multiverse
deb-src http://mirrors.aliyun.com/ubuntu/ bionic-proposed main restricted universe multiverse
deb-src http://mirrors.aliyun.com/ubuntu/ bionic-backports main restricted universe multiverse
#LLVM
deb [trusted=yes] http://apt.llvm.org/bionic/ llvm-toolchain-bionic-12 main
deb-src [trusted=yes] http://apt.llvm.org/bionic/ llvm-toolchain-bionic-12 main
```
Step 2: add configuration below to `/etc/apt/apt.conf`
```
Acquire::https::apt.llvm.org::Verify-Peer "false";
```
Step 3: update sources
```
wget --no-check-certificate -O - https://apt.llvm.org/llvm-snapshot.gpg.key|sudo apt-key add -
apt-get update
```

###Install autoconf
```
apt-get install autoconf
```

###Install cmake
```
apt-get install cmake
```

###Install jdk
```
apt-get install openjdk-8-jdk
```
Add the below instruction to `/etc/profile`:
```
export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-amd64
```

###Install LLVM
We install LLVM-12 since OmniRuntime uses LLVM-12 now. Reference: https://apt.llvm.org/. Follow instructions below to install LLVM:
```
apt-get install libllvm-12-ocaml-dev libllvm12 llvm-12 llvm-12-dev llvm-12-doc llvm-12-examples llvm-12-runtime
apt-get install clang-12 clang-tools-12 clang-12-doc libclang-common-12-dev libclang-12-dev libclang1-12 clang-format-12 clangd-12
```

###Install git
```
apt-get install git
```

###Install jemalloc
```
git clone -b 5.2.0 https://github.com/jemalloc/jemalloc.git
cd jemalloc
./autogen.sh --disable-initial-exec-tls
make
make install
```
Add the below instruction to `/etc/profile`:
```
export LD_LIBRARY_PATH=/usr/local/lib
export LD_PRELOAD=/usr/local/lib/libjemalloc.so
```

###Install GoogleTest
```
git clone https://github.com/google/googletest.git -b release-1.10.0
cd googletest        # Main directory of the cloned repository.
mkdir build          # Create a directory to hold the build output.
cd build
cmake ..             # Generate native build scripts for GoogleTest.
make
make install
```

###Install json
```
git clone https://github.com/nlohmann/json.git -b v3.9.1
mkdir build
cd build
cmake ../
make -j4
make install
```

###Install maven
```
apt-get install maven
```
Add a mirror in ***settings.xml***
```
  <mirror>
      <id>datacloudFI</id>
      <mirrorOf>*</mirrorOf>
      <name>datacloudFI</name>
      <url>http://10.244.95.155:8089/nexus/content/groups/datacloudFI/</url>
  </mirror>
```

###Install protobuf
```
apt-get install protobuf-compiler
```

#Compile omniruntime And OLK

##Download source code
Download OmniRuntime code from Code-Hub:
```
git clone https://codehub-dg-y.huawei.com/data-app-lab/omni-runtime.git
```
Download Hetu-core code from Code-Hub:
```
git clone https://codehub-dg-y.huawei.com/data-app-lab/hetu-core.git -b omni-runtime-integration
```

##Compile code
You'd better build the project by following order:

###Compile bindings/java
```
cd omni-runtime/bindings/java
mvn clean install -DskipTests
```

###Compile OmniRuntime native code
```
cd omni-runtime/core/build
```
You can compile OmniRuntime in ***RELEASE***, ***DEBUG*** or ***TRACE*** mode. <br/>
***RELEASE***:<br/>
```
sh build.sh release [--disable-jit]
```
***DEBUG***:<br/>
```
sh build.sh debug [op/vec/llvm/all] [--disable-jit]
```
***TRACE***:<br/>
```
sh build.sh trace [op/vec/llvm/all] [--disable-jit]
```
Parameters explanation:<br/>
+ op: debug or trace operator
+ vec: debug or trace vector
+ llvm: debug or trace llvm
+ all: debug or trace operator, vector and llvm
+ --disable-jit: disable JIT

If the compilation mode is not specified, OmniRuntime will be compiled in ***RELEASE*** mode, such as `sh build.sh`.

###Compile hetu-core
```
cd hetu-core/
mvn clean install -DskipTests
```

###Compile the adapter
```
cd omni-runtime/adapter/openlookeng
mvn clean install -DskipTests
```

#Run OLK with OmniRuntime
1. Please configuring openLooKeng with reference https://openlookeng.io/docs/docs/installation/deployment.html.
2. Please refer to the [How to Use openLooKeng adapter](./adapter/openlookeng/ReadMe.md) to setup the additional config for openLooKeng when using OmniRuntime.
3. Add the below instruction to `/etc/profile` for searching libomni_runtime.so: <br/>
      `export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/opt/lib` <br/>
      Or you can setup as follows if you want install the dependent libraries in the specified path: <br/>
      ```
      export OMNI_HOME=xxPath
      export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$OMNI_HOME
      ```
4. Run OLK.

#Common issues
1. ***Error***: fatal, unable to access 'https://github.com/jemalloc/jemalloc.git/': server certificate verification failed. CAfile: /etc/ssl/certs/ca-certificates.crt CRLfile: none. <br/>
      ***Solution***: `export GIT_SSL_NO_VERIFY=1`
2. ***Error***: fatal, unable to access 'https://codehub-dg-y.huawei.com/data-app-lab/omni-runtime.git/': Received HTTP code 504 from proxy after CONNECT. <br/>
      ***Solution***: `unset http_proxy; unset https_proxy`
3. ***Tip***: If your username or password has some special characters, please add `'\'` before them when configuring proxy.




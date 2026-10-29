# OmniOperator Source Build Guide<a name="source_build_guide"></a>

## Build Overview<a name="build_overview"></a>

This document describes the complete procedure for building OmniOperator from source on the internet_commercial branch in the openEuler 24.03 LTS aarch64 environment. The procedure is organized in the sequence of environment preparation, source package preparation, dependency installation, build and packaging, and installation verification. Before starting, check the environment against [**Table 1** Environment requirements](#build_environment_requirements), and then perform the operations in the following sections in order.

All commands in this document are executed by the root user, and the working directory is `/opt/omni`.

**Table 1** Environment requirements<a id="build_environment_requirements"></a>

|Item|Requirement|
|--|--|
|OS|openEuler 24.03 LTS|
|Processor architecture|aarch64|
|User permission|root|
|Hardware resources|32 CPU cores or more, 64 GB memory or more, and 40 GB available disk space or more are recommended|
|Network|Accessible to `repo.openeuler.org`, `gitcode.com`, `gitee.com`, and Maven mirror sites|
|Local files to upload|None. All source code is downloaded or cloned on the server|

>![](public_sys-resources/icon-note.gif) **NOTE:**
>If GitHub is not directly accessible, use the `https://ghfast.top/` proxy, which has been included in the related commands below.

## System Preparation<a name="system_preparation"></a>

### Install the Compilation Toolchain<a name="install_toolchain"></a>

1. Install the basic compilation toolchain.

    ```shell
    dnf install -y git gcc gcc-c++ make cmake ninja-build autoconf automake libtool m4 \
      perl python3 curl wget tar gzip bzip2 xz unzip zip file which findutils procps-ng \
      util-linux patch openssl openssl-devel zlib-devel libffi-devel ncurses-devel krb5-devel \
      java-1.8.0-openjdk-devel
    ```

2. Install the development headers of the compression libraries required by Arrow.

    ```shell
    dnf install -y utf8proc-devel libcurl-devel boost-devel bison flex \
      brotli-devel bzip2-devel lz4-devel snappy-devel libzstd-devel
    ```

    >![](public_sys-resources/icon-note.gif) **NOTE:**
    >Arrow depends on the development headers of the preceding compression libraries during compilation. If any of them is missing, the CMake phase fails with a dependency-not-found error.

3. Install cyrus-sasl-devel.

    ```shell
    dnf install -y cyrus-sasl-devel
    ```

    >![](public_sys-resources/icon-note.gif) **NOTE:**
    >Hadoop libhdfs++ needs to locate the SASL library during compilation. If cyrus-sasl-devel is missing, the following error is reported: "Could not find a SASL library (GSASL (gsasl) or Cyrus SASL (libsasl2)".

4. Verify that the compilation toolchain is installed.

    ```shell
    java -version          # The version is 1.8.0_462.
    mvn -v 2>/dev/null     # Maven is not installed yet. See the Install Maven section.
    ninja --version
    ```

### Create Directories<a name="create_directories"></a>

1. Create the directories used during compilation.

    ```shell
    mkdir -p /opt/omni/{src,downloads,logs,tools,adaptor}
    mkdir -p /opt/omni/downloads/arrow-thirdparty
    ```

2. Create the `/opt/Adaptor` symbolic link, which is referenced during Hadoop installation and compilation.

    ```shell
    ln -sfn /opt/omni/adaptor /opt/Adaptor
    ```

### Configure the Dynamic Library Search Path<a name="configure_ld_so"></a>

The default `/etc/ld.so.conf` of openEuler does not contain `/usr/local/lib` or `/usr/local/lib64`, while the dependencies installed in this document are located in these two directories. Therefore, add them to the dynamic library search path manually.

```shell
cat > /etc/ld.so.conf.d/usr-local.conf <<'EOF'
/usr/local/lib
/usr/local/lib64
EOF
ldconfig
```

### Install Maven<a name="install_maven"></a>

1. Download and decompress Maven 3.8.3.

    ```shell
    curl -sSL -o /opt/omni/downloads/apache-maven-3.8.3-bin.tar.gz \
      https://mirrors.huaweicloud.com/apache/maven/maven-3/3.8.3/binaries/apache-maven-3.8.3-bin.tar.gz
    tar -xzf /opt/omni/downloads/apache-maven-3.8.3-bin.tar.gz -C /opt/omni/tools
    ```

2. Configure the Huawei Cloud central repository mirror in `/root/.m2/settings.xml`.

    ```shell
    mkdir -p /root/.m2
    cat > /root/.m2/settings.xml <<'EOF'
    <settings>
      <mirrors>
        <mirror>
          <id>huaweicloud</id>
          <mirrorOf>central</mirrorOf>
          <url>https://mirrors.huaweicloud.com/repository/maven/</url>
        </mirror>
      </mirrors>
    </settings>
    EOF
    ```

### Generate the Environment Variable Script<a name="configure_env_script"></a>

1. Create the environment variable script `/opt/omni/env.sh`.

    ```shell
    cat > /opt/omni/env.sh <<'EOF'
    #!/bin/bash
    export WORK=/opt/omni
    export SRC=/opt/omni/src
    export LOGS=/opt/omni/logs
    export OMNI_SRC=/opt/omni/src/OmniOperator
    export OMNI_HOME=/opt/omni/install
    export DEPS=/usr/local
    export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk
    export MAVEN_HOME=/opt/omni/tools/apache-maven-3.8.3
    export LLVM_HOME=/usr/lib/llvm-15
    export PROTOBUF_HOME=/opt/Gluten
    export OMNI_COMPILER_THREAD_COUNT=64
    export PATH="$PROTOBUF_HOME/bin:$MAVEN_HOME/bin:$LLVM_HOME/bin:/usr/local/bin:$JAVA_HOME/bin:$PATH"
    export CMAKE_PREFIX_PATH="/usr/local:/opt/Gluten:${CMAKE_PREFIX_PATH:-}"
    export C_INCLUDE_PATH="$DEPS/include:$DEPS/include/libboundscheck/include:$LLVM_HOME/include"
    export CPLUS_INCLUDE_PATH="$C_INCLUDE_PATH"
    export LIBRARY_PATH="$OMNI_HOME/lib:$DEPS/lib:$DEPS/lib64:$LLVM_HOME/lib:$JAVA_HOME/jre/lib/aarch64/server"
    export LD_LIBRARY_PATH="$OMNI_HOME/lib:$DEPS/lib:$DEPS/lib64:$LLVM_HOME/lib:$JAVA_HOME/jre/lib/aarch64/server"
    log() { echo "[$(date '+%F %T')] $*"; }
    EOF
    ```

2. Grant the execute permission on the script and make it take effect.

    ```shell
    chmod +x /opt/omni/env.sh
    source /opt/omni/env.sh
    ```

>![](public_sys-resources/icon-notice.gif) **NOTICE:**
>`JAVA_HOME` must use the alternatives symbolic link `/usr/lib/jvm/java-1.8.0-openjdk`, instead of the actual directory with the build number.

### Clone Patch Repositories<a name="clone_patch_repos"></a>

1. Clone the patch repositories and the snappy source repository.

    ```shell
    cd /opt/omni/src
    git clone --depth 1 --branch main https://gitee.com/kunpengcompute/boostkit-bigdata.git
    git clone --depth 1 --branch openEuler-24.03-LTS-SP1-release https://gitee.com/src-openeuler/snappy.git snappy-src
    ```

    >![](public_sys-resources/icon-note.gif) **NOTE:**
    >`boostkit-bigdata` provides the patches for Arrow and Hadoop, and `snappy-src` provides the snappy source code and two patches.

2. Handle offline environments. Run the preceding clone commands on a machine with network access, package the repositories, copy them to the `/opt/omni/src` directory, and decompress them.

## Prepare Source Packages<a name="prepare_source_packages"></a>

### Download Source Packages<a name="download_source_packages"></a>

1. Download all source packages.

    ```shell
    DL=/opt/omni/downloads
    TP=$DL/arrow-thirdparty
    GH="https://ghfast.top/https://github.com"
    cd "$DL"

    get() { curl -fL --retry 3 --connect-timeout 20 -o "$2" "$1" || echo "FAILED: $2"; }

    get "$GH/apache/arrow/archive/refs/tags/apache-arrow-11.0.0.tar.gz"                arrow-apache-arrow-11.0.0.tar.gz
    get "$GH/nlohmann/json/archive/refs/tags/v3.11.3.tar.gz"                           json-v3.11.3.tar.gz
    get "$GH/jemalloc/jemalloc/archive/refs/tags/5.3.0.tar.gz"                         jemalloc-5.3.0.tar.gz
    get "$GH/madler/zlib/archive/refs/tags/v1.3.1.tar.gz"                              zlib-v1.3.1.tar.gz
    get "$GH/lz4/lz4/archive/refs/tags/v1.10.0.tar.gz"                                 lz4-v1.10.0.tar.gz
    get "$GH/facebook/zstd/archive/refs/tags/v1.5.6.tar.gz"                            zstd-v1.5.6.tar.gz
    get "$GH/cyrusimap/cyrus-sasl/archive/refs/tags/cyrus-sasl-2.1.28.tar.gz"         cyrus-sasl-2.1.28.tar.gz
    get "$GH/zlib-ng/zlib-ng/archive/refs/tags/2.0.4.tar.gz"                           zlib-ng-2.0.4.tar.gz
    get "$GH/open-source-parsers/jsoncpp/archive/refs/tags/1.9.6.tar.gz"               jsoncpp-1.9.6.tar.gz
    get "$GH/apache/hadoop/archive/refs/tags/rel/release-3.2.0.tar.gz"                 hadoop-rel-release-3.2.0.tar.gz
    get "$GH/google/googletest/archive/refs/tags/v1.14.0.tar.gz"                       googletest-v1.14.0.tar.gz
    get "$GH/abseil/abseil-cpp/archive/refs/tags/20240722.0.tar.gz"                    abseil-cpp-20240722.0.tar.gz
    get "$GH/google/re2/archive/refs/tags/2024-07-02.tar.gz"                           re2-2024-07-02.tar.gz
    get "$GH/protocolbuffers/protobuf/archive/refs/tags/v3.21.9.tar.gz"                protobuf-v3.21.9.tar.gz
    get "$GH/Tencent/rapidjson/archive/refs/tags/v1.1.0.tar.gz"                        rapidjson-v1.1.0.tar.gz
    get "$GH/facebook/folly/archive/refs/tags/v2024.01.01.00.tar.gz"                   folly-v2024.01.01.00.tar.gz
    get "$GH/llvm/llvm-project/releases/download/llvmorg-15.0.4/llvm-project-15.0.4.src.tar.xz" \
        llvm-project-15.0.4.src.tar.xz

    # Third-party packages used during Arrow build
    get "$GH/microsoft/mimalloc/archive/refs/tags/v2.0.6.tar.gz"                       "$TP/mimalloc-v2.0.6.tar.gz"
    get "$GH/substrait-io/substrait/archive/refs/tags/v0.20.0.tar.gz"                  "$TP/substrait-v0.20.0.tar.gz"
    get "$GH/google/re2/archive/refs/tags/2022-06-01.tar.gz"                           "$TP/re2-2022-06-01.tar.gz"
    get "$GH/apache/thrift/archive/refs/tags/v0.16.0.tar.gz"                           "$TP/thrift-0.16.0.tar.gz"
    get "$GH/xtensor-stack/xsimd/archive/refs/tags/9.0.1.tar.gz"                       "$TP/xsimd-9.0.1.tar.gz"
    ```

    >![](public_sys-resources/icon-note.gif) **NOTE:**
    >The thrift tag has a `v` prefix. If it is written as `0.16.0`, the request returns 404. The top-level directory of the archive is still `thrift-0.16.0`.

2. Check the download result to confirm the file size and integrity. Files that fail to be downloaded are marked with "FAILED".

    ```shell
    ls -l $DL/*.tar.* $DL/*.xz $TP/*.tar.gz
    ```

### Repack Protobuf v21.3<a name="repack_protobuf"></a>

The ExternalProject of Arrow requires the top-level directory of the archive to be `protobuf-21.3`. Therefore, clone the v21.3 branch from GitHub and repack it.

```shell
cd /opt/omni/src
rm -rf protobuf-21.3
git clone --depth 1 -b v21.3 https://github.com/protocolbuffers/protobuf.git protobuf-21.3

tar -czf /opt/omni/downloads/arrow-thirdparty/protobuf-v21.3.tar.gz protobuf-21.3
sha256sum /opt/omni/downloads/arrow-thirdparty/protobuf-v21.3.tar.gz
```

>![](public_sys-resources/icon-note.gif) **NOTE:**
>The clone directory is named `protobuf-21.3`, so the top-level directory of the repacked archive meets the Arrow naming requirement. The [Install Arrow](#install_arrow) section recalculates the checksum and writes it to `versions.txt` accordingly.
>![](public_sys-resources/icon-notice.gif) **NOTICE:**
>The Protobuf v21.3 here is used only for the internal build of Arrow. Do not confuse it with the Protobuf 3.21.9 installed on the system (see [Install Protobuf 3.21.9](#install_protobuf)).

### Repack jemalloc 5.3.0<a name="repack_jemalloc"></a>

The GitHub archive does not contain the `configure` script, and the ExternalProject of Arrow does not run `autogen.sh` automatically. Therefore, generate `configure` first and then repack the source code.

```shell
cd /opt/omni/src
rm -rf jemalloc-tp && mkdir jemalloc-tp
tar -xzf /opt/omni/downloads/jemalloc-5.3.0.tar.gz -C jemalloc-tp --strip-components=1

cd jemalloc-tp
echo "5.3.0" > VERSION
./autogen.sh --disable-initial-exec-tls --with-lg-page=16

cd /opt/omni/src
tar -czf /opt/omni/downloads/arrow-thirdparty/jemalloc-5.3.0.tar.gz \
  --transform 's,^jemalloc-tp,jemalloc-5.3.0,' jemalloc-tp
sha256sum /opt/omni/downloads/arrow-thirdparty/jemalloc-5.3.0.tar.gz
```

### Obtain ORC Source Code<a name="obtain_orc"></a>

The ORC source code is hosted in the public [repository](https://gitcode.com/boostkit/orc). Clone the `v1.7.4_new` branch directly.

```shell
cd /opt/omni/src
rm -rf orc
git clone -b v1.7.4_new https://gitcode.com/boostkit/orc.git orc
```

>![](public_sys-resources/icon-note.gif) **NOTE:**
>The top-level directory of the cloned `orc` repository is the root directory of the ORC source code (containing `CMakeLists.txt`, `c++`, `proto`, `tools`, and so on). The [Install ORC 1.7.4](#install_orc) section builds ORC in the `/opt/omni/src/orc` directory.

## Install Common Dependencies<a name="install_common_dependencies"></a>

This section describes how to install the common dependencies required for building OmniOperator. Perform the operations in the following sections in order.

### Install Protobuf 3.21.9<a name="install_protobuf"></a>

Hadoop libhdfspp, ORC, and the protoc plug-in of Maven all depend on Protobuf. In this document, Protobuf is installed in the `/opt/Gluten` directory.

1. Build and install Protobuf.

    ```shell
    cd /opt/omni/src
    rm -rf protobuf && mkdir protobuf
    tar -xzf /opt/omni/downloads/protobuf-v3.21.9.tar.gz -C protobuf --strip-components=1

    cd protobuf
    ./autogen.sh
    ./configure --enable-static=no --prefix=/opt/Gluten
    make -j32
    make install
    ```

2. Verify that Protobuf is installed.

    ```shell
    /opt/Gluten/bin/protoc --version    # The version is libprotoc 3.21.9.
    ```

### Install Arrow 11.0.0<a name="install_arrow"></a>

1. Decompress the source package and apply the patch.

    ```shell
    cd /opt/omni/src
    rm -rf arrow && mkdir arrow
    tar -xzf /opt/omni/downloads/arrow-apache-arrow-11.0.0.tar.gz -C arrow --strip-components=1

    cp boostkit-bigdata/omnioperator/contrib/arrow-maint-11_0_0/arrow-maint-11_0_0.patch arrow/
    cd arrow
    patch -p1 < arrow-maint-11_0_0.patch
    ```

2. Point the third-party packages to local files and write back the checksums.

    >![](public_sys-resources/icon-note.gif) **NOTE:**
    >The third-party packages such as mimalloc, protobuf, and jemalloc are repacked locally, so their SHA256 values are different from the official checksums. Recalculate the values and write them to `cpp/thirdparty/versions.txt`; otherwise, the build fails directly.

    ```shell
    TP=/opt/omni/downloads/arrow-thirdparty

    fix_sha() {
      local key=$1 file=$2
      local sha
      sha=$(sha256sum "$file" | awk '{print $1}')
      sed -i "s|^${key}=.*|${key}=${sha}|" cpp/thirdparty/versions.txt
      echo "$key=$sha"
    }

    fix_sha ARROW_MIMALLOC_BUILD_SHA256_CHECKSUM  "$TP/mimalloc-v2.0.6.tar.gz"
    fix_sha ARROW_PROTOBUF_BUILD_SHA256_CHECKSUM  "$TP/protobuf-v21.3.tar.gz"
    fix_sha ARROW_SUBSTRAIT_BUILD_SHA256_CHECKSUM "$TP/substrait-v0.20.0.tar.gz"
    fix_sha ARROW_JEMALLOC_BUILD_SHA256_CHECKSUM  "$TP/jemalloc-5.3.0.tar.gz"
    fix_sha ARROW_RE2_BUILD_SHA256_CHECKSUM       "$TP/re2-2022-06-01.tar.gz"
    fix_sha ARROW_THRIFT_BUILD_SHA256_CHECKSUM    "$TP/thrift-0.16.0.tar.gz"
    fix_sha ARROW_XSIMD_BUILD_SHA256_CHECKSUM     "$TP/xsimd-9.0.1.tar.gz"
    ```

3. Build and install Arrow.

    >![](public_sys-resources/icon-notice.gif) **NOTICE:**
    >`ARROW_DATASET` must be set to `ON` because the `reader` module depends on `ArrowDataset`.

    ```shell
    export ARROW_MIMALLOC_URL=file://$TP/mimalloc-v2.0.6.tar.gz
    export ARROW_PROTOBUF_URL=file://$TP/protobuf-v21.3.tar.gz
    export ARROW_SUBSTRAIT_URL=file://$TP/substrait-v0.20.0.tar.gz
    export ARROW_JEMALLOC_URL=file://$TP/jemalloc-5.3.0.tar.gz
    export ARROW_RE2_URL=file://$TP/re2-2022-06-01.tar.gz
    export ARROW_THRIFT_URL=file://$TP/thrift-0.16.0.tar.gz
    export ARROW_XSIMD_URL=file://$TP/xsimd-9.0.1.tar.gz

    mkdir -p cpp/_build && cd cpp/_build
    cmake .. \
      -DCMAKE_BUILD_TYPE=Release \
      -DARROW_JEMALLOC_LG_PAGE=16 \
      -DARROW_BUILD_STATIC=OFF \
      -DARROW_BUILD_TESTS=OFF \
      -DARROW_ENABLE_TIMING_TESTS=OFF \
      -DARROW_BUILD_INTEGRATION=OFF \
      -DARROW_COMPUTE=ON \
      -DARROW_DATASET=ON \
      -DARROW_EXTRA_ERROR_CONTEXT=ON \
      -DARROW_MIMALLOC=ON \
      -DARROW_SUBSTRAIT=ON \
      -DARROW_WITH_BROTLI=ON -DARROW_WITH_BZ2=ON -DARROW_WITH_LZ4=ON \
      -DARROW_WITH_SNAPPY=ON -DARROW_WITH_UTF8PROC=ON -DARROW_WITH_ZLIB=ON \
      -DARROW_WITH_ZSTD=ON -DARROW_HDFS=ON
    make -j32
    make install
    ```

4. Verify that Arrow is installed.

    ```shell
    grep ARROW_VERSION_STRING /usr/local/include/arrow/util/config.h   # "11.0.0"
    ls /usr/local/lib64/cmake/ArrowDataset
    ```

### Install nlohmann/json 3.11.3<a name="install_json"></a>

```shell
cd /opt/omni/src
rm -rf json && mkdir json
tar -xzf /opt/omni/downloads/json-v3.11.3.tar.gz -C json --strip-components=1
cmake -S json -B json/build -DCMAKE_BUILD_TYPE=Release -DJSON_BuildTests=OFF -DJSON_Install=ON
cmake --build json/build --parallel 32
cmake --install json/build
```

### Install jemalloc 5.3.0<a name="install_jemalloc"></a>

```shell
cd /opt/omni/src
rm -rf jemalloc && mkdir jemalloc
tar -xzf /opt/omni/downloads/arrow-thirdparty/jemalloc-5.3.0.tar.gz -C jemalloc --strip-components=1
cd jemalloc
./autogen.sh --disable-initial-exec-tls --with-lg-page=16
make -j32
make install
```

Verify that jemalloc is installed. Run `ldconfig -p | grep jemalloc` to query the library. The header file is located in `/usr/local/include/jemalloc/jemalloc.h`.

### Install zlib 1.3.1<a name="install_zlib"></a>

```shell
cd /opt/omni/src
rm -rf zlib && mkdir zlib
tar -xzf /opt/omni/downloads/zlib-v1.3.1.tar.gz -C zlib --strip-components=1
cd zlib
./configure --prefix=/usr/local
make -j32
make install
```

### Install LZ4 1.10.0<a name="install_lz4"></a>

```shell
cd /opt/omni/src
rm -rf lz4 && mkdir lz4
tar -xzf /opt/omni/downloads/lz4-v1.10.0.tar.gz -C lz4 --strip-components=1
cd lz4
make -j32
make install PREFIX=/usr/local
```

### Install Snappy 1.1.10<a name="install_snappy"></a>

```shell
cd /opt/omni/src
rm -rf snappy && mkdir snappy
tar -xzf snappy-src/snappy-1.1.10.tar.gz -C snappy --strip-components=1

cd snappy
patch -p1 < ../snappy-src/add-option-to-enable-rtti-set-default-to-current-ben.patch
patch -p1 < ../snappy-src/remove-dependency-on-google-benchmark-and-gmock.patch

rm -rf build && mkdir build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release \
  -DSNAPPY_BUILD_BENCHMARKS=OFF -DSNAPPY_BUILD_TESTS=OFF \
  -DBUILD_SHARED_LIBS=ON -DCMAKE_POSITION_INDEPENDENT_CODE=ON
make -j32
make install
```

### Install Zstandard 1.5.6<a name="install_zstd"></a>

```shell
cd /opt/omni/src
rm -rf zstd && mkdir zstd
tar -xzf /opt/omni/downloads/zstd-v1.5.6.tar.gz -C zstd --strip-components=1
cd zstd
make -j32 PREFIX=/usr/local
make install PREFIX=/usr/local
```

### Install Cyrus SASL 2.1.28<a name="install_cyrus_sasl"></a>

```shell
cd /opt/omni/src
rm -rf cyrus-sasl && mkdir cyrus-sasl
tar -xzf /opt/omni/downloads/cyrus-sasl-2.1.28.tar.gz -C cyrus-sasl --strip-components=1
cd cyrus-sasl
./autogen.sh
make -j32
make install
```

### Install zlib-ng 2.0.4<a name="install_zlib_ng"></a>

```shell
cd /opt/omni/src
rm -rf zlib-ng && mkdir zlib-ng
tar -xzf /opt/omni/downloads/zlib-ng-2.0.4.tar.gz -C zlib-ng --strip-components=1
cd zlib-ng
./configure --prefix=/usr/local
rm -rf build && mkdir build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release -DBUILD_SHARED_LIBS=OFF -DCMAKE_POSITION_INDEPENDENT_CODE=ON
make -j32
make install
```

### Install jsoncpp 1.9.6<a name="install_jsoncpp"></a>

```shell
cd /opt/omni/src
rm -rf jsoncpp && mkdir jsoncpp
tar -xzf /opt/omni/downloads/jsoncpp-1.9.6.tar.gz -C jsoncpp --strip-components=1
rm -rf jsoncpp/build && mkdir -p jsoncpp/build && cd jsoncpp/build
cmake .. -DCMAKE_BUILD_TYPE=Release -DBUILD_STATIC_LIBS=ON -DBUILD_SHARED_LIBS=OFF
make -j32
make install
```

### Install Hadoop 3.2.0<a name="install_hadoop"></a>

1. Decompress the source package and apply the patch.

    ```shell
    source /opt/omni/env.sh

    cd /opt/omni/src
    rm -rf hadoop && mkdir hadoop
    tar -xzf /opt/omni/downloads/hadoop-rel-release-3.2.0.tar.gz -C hadoop --strip-components=1

    cp boostkit-bigdata/omnioperator/contrib/hadoop-3_2_0/hadoop-3_2_0.patch hadoop/
    cd hadoop
    patch -p1 < hadoop-3_2_0.patch
    ```

2. Build the native part of Hadoop.

    ```shell
    cd /opt/omni/src/hadoop/hadoop-hdfs-project/hadoop-hdfs-native-client
    mvn clean package -DskipTests -Pdist,native -Dtar
    ```

3. Install the header files and libraries to `/opt/Adaptor`.

    ```shell
    cd /opt/omni/src/hadoop/hadoop-hdfs-project/hadoop-hdfs-native-client
    mkdir -p /opt/Adaptor/include
    cp -rf src/main/native/libhdfs/include/* /opt/Adaptor/include/
    cp target/hadoop-hdfs-native-client-3.2.0/include/hdfs.h /opt/Adaptor/include/   # If the file does not exist, use target/target/usr/local/include/hdfs.h.
    cp -rf src/main/native/libhdfspp/include /opt/Adaptor/
    cp -rf target/target/usr/local/lib /opt/Adaptor/
    cp -f target/main/native/libhdfspp/libhdfspp.so* /opt/Adaptor/lib/
    ```

4. Verify that Hadoop is installed.

    ```shell
    ls /opt/Adaptor/lib       # libhdfs.so.0.0.0, libhdfspp.so.0.1.0
    ls /opt/Adaptor/include   # hdfs  hdfs.h  hdfspp
    ```

### Install GoogleTest 1.14.0<a name="install_googletest"></a>

```shell
cd /opt/omni/src
rm -rf googletest && mkdir googletest
tar -xzf /opt/omni/downloads/googletest-v1.14.0.tar.gz -C googletest --strip-components=1
cmake -S googletest -B googletest/build -DCMAKE_BUILD_TYPE=Release
cmake --build googletest/build --parallel 32
cmake --install googletest/build
```

### Install Abseil-cpp 20240722.0<a name="install_abseil"></a>

```shell
cd /opt/omni/src
rm -rf abseil-cpp && mkdir abseil-cpp
tar -xzf /opt/omni/downloads/abseil-cpp-20240722.0.tar.gz -C abseil-cpp --strip-components=1
cmake -S abseil-cpp -B abseil-cpp/build -DCMAKE_BUILD_TYPE=Release \
  -DBUILD_SHARED_LIBS=ON -DCMAKE_POSITION_INDEPENDENT_CODE=ON
cmake --build abseil-cpp/build --parallel 32
cmake --install abseil-cpp/build
```

### Install RE2 2024-07-02<a name="install_re2"></a>

```shell
cd /opt/omni/src
rm -rf re2 && mkdir re2
tar -xzf /opt/omni/downloads/re2-2024-07-02.tar.gz -C re2 --strip-components=1
cmake -S re2 -B re2/build -DCMAKE_BUILD_TYPE=Release \
  -DBUILD_SHARED_LIBS=ON -DCMAKE_PREFIX_PATH=/usr/local
cmake --build re2/build --parallel 32
cmake --install re2/build
```

### Install libboundscheck<a name="install_libboundscheck"></a>

`vector`, `util`, and `codegen` are all linked with `-lboundscheck`, and the include path of the source code is `<libboundscheck/include/securec.h>`. Therefore, the header file directory hierarchy must be retained during installation.

1. Build and install libboundscheck.

    ```shell
    cd /opt/omni/src
    rm -rf libboundscheck
    git clone https://gitcode.com/openeuler/libboundscheck.git libboundscheck

    cd libboundscheck
    git checkout 6ad12812c133297b1580d7da6e197af3bae9c749
    make CC=gcc -j32

    mkdir -p /usr/local/include/libboundscheck/include /usr/local/lib
    cp -a include/. /usr/local/include/libboundscheck/include/
    cp -f lib/libboundscheck.so /usr/local/lib/
    ldconfig
    ```

2. Verify that libboundscheck is installed.

    ```shell
    ls /usr/local/lib/libboundscheck.so
    ls /usr/local/include/libboundscheck/include/securec.h
    ```

### Install RapidJSON 1.1.0<a name="install_rapidjson"></a>

RapidJSON is a header-only library. You only need to copy the header files.

```shell
cd /opt/omni/src
tar -zxf /opt/omni/downloads/rapidjson-v1.1.0.tar.gz -C /opt/omni/src
mkdir -p /usr/local/include
cp -a /opt/omni/src/rapidjson-1.1.0/include/rapidjson /usr/local/include/
```

Verify that RapidJSON is installed. If the `/usr/local/include/rapidjson/document.h` file exists, the installation is successful.

### Install LLVM/Clang 15.0.4<a name="install_llvm"></a>

The `codegen` module hardcodes the `/usr/lib/llvm-15/include` directory and links `-lLLVM-15`. Therefore, LLVM must be installed in this path, and the dynamic library must be generated.

1. Build and install LLVM/Clang.

    ```shell
    cd /opt/omni/src
    tar -xf /opt/omni/downloads/llvm-project-15.0.4.src.tar.xz
    # The top-level directory is llvm-project-15.0.4.src.

    rm -rf /opt/omni/src/llvm-build-15.0.4
    cmake -G Ninja -S /opt/omni/src/llvm-project-15.0.4.src/llvm \
      -B /opt/omni/src/llvm-build-15.0.4 \
      -DCMAKE_BUILD_TYPE=Release \
      -DLLVM_TARGETS_TO_BUILD="AArch64" \
      -DLLVM_ENABLE_PROJECTS="clang" \
      -DLLVM_ENABLE_RTTI=ON \
      -DLLVM_BUILD_LLVM_DYLIB=ON \
      -DLLVM_LINK_LLVM_DYLIB=ON \
      -DLLVM_INSTALL_UTILS=ON \
      -DLLVM_INCLUDE_TESTS=OFF \
      -DLLVM_INCLUDE_EXAMPLES=OFF \
      -DLLVM_INCLUDE_BENCHMARKS=OFF \
      -DLLVM_INCLUDE_DOCS=OFF \
      -DCMAKE_INSTALL_PREFIX=/usr/lib/llvm-15

    cmake --build /opt/omni/src/llvm-build-15.0.4 --parallel 64
    cmake --install /opt/omni/src/llvm-build-15.0.4
    ```

    >![](public_sys-resources/icon-notice.gif) **NOTICE:**
    >Both `LLVM_ENABLE_RTTI=ON` and `LLVM_BUILD_LLVM_DYLIB=ON` are mandatory. Otherwise, the compilation or linking of codegen fails.

2. Configure the dynamic library path and the symbolic links available to clang++.

    ```shell
    ln -sf /usr/lib/llvm-15/lib/libLLVM-15.so /usr/local/lib64/libLLVM-15.so

    echo /usr/lib/llvm-15/lib > /etc/ld.so.conf.d/llvm-15.conf
    ldconfig

    for b in clang clang++ llvm-config llvm-ar llvm-link opt llc llvm-as; do
      ln -sf /usr/lib/llvm-15/bin/$b /usr/local/bin/$b
    done

    # The GCC triple of openEuler is aarch64-openEuler-linux, while clang 15 looks for aarch64-unknown-linux-gnu by default.
    ln -sfn /usr/lib/gcc/aarch64-openEuler-linux /usr/lib/gcc/aarch64-unknown-linux-gnu
    ln -sfn /usr/include/c++/12/aarch64-openEuler-linux /usr/include/c++/12/aarch64-unknown-linux-gnu
    ```

3. Verify that LLVM/Clang is installed.

    ```shell
    llvm-config --version        # 15.0.4
    clang --version | head -1
    ldconfig -p | grep libLLVM-15
    ```

## Install Additional Dependencies for the internet_commercial Branch<a name="install_extra_dependencies"></a>

The `vectorization` and `reader` modules introduce the following dependencies, which are not included in the common dependencies and need to be installed separately.

### Install System Packages<a name="install_extra_system_packages"></a>

```shell
dnf install -y fmt-devel double-conversion-devel gflags-devel glog-devel \
  libevent-devel libunwind-devel libsodium-devel boost-devel lld
```

>![](public_sys-resources/icon-notice.gif) **NOTICE:**
>`lld` is mandatory for `build.sh`, which uses `-fuse-ld=lld` for linking.

### Install folly<a name="install_folly"></a>

The repository does not provide the `folly-devel` package, so folly must be built from source.

1. Build folly.

    ```shell
    cd /opt/omni/src
    rm -rf folly && mkdir folly
    tar -xzf /opt/omni/downloads/folly-v2024.01.01.00.tar.gz -C folly --strip-components=1

    # The build directory cannot be named build:
    # build/fbcode_builder/CMake/FBBuildOptions.cmake in the folly source code is a build dependency.
    cd folly && rm -rf _build && mkdir _build && cd _build

    cmake .. \
      -DCMAKE_BUILD_TYPE=Release \
      -DCMAKE_INSTALL_PREFIX=/usr/local \
      -DBUILD_SHARED_LIBS=ON \
      -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
      -DCMAKE_PREFIX_PATH="/usr/local;/usr/lib64/cmake;/usr/lib64" \
      -DCMAKE_CXX_STANDARD=17

    make -j32
    ```

2. Install folly.

    >![](public_sys-resources/icon-note.gif) **NOTE:**
    >The sample program `logging_example` may fail to compile due to a snappy symbolic link issue. If `libfolly.so` has been generated, skip the sample and install folly directly.

    ```shell
    cmake --install .
    ldconfig
    ```

3. Verify that folly is installed. If the `/usr/local/lib/libfolly.so` file and the `/usr/local/lib/cmake/folly` directory exist, the installation is successful.

### Install ORC 1.7.4<a name="install_orc"></a>

The `reader` module strongly depends on ORC, so ORC must be installed first. The ORC source code has been cloned to `/opt/omni/src/orc` in the [Obtain ORC Source Code](#obtain_orc) section, and its top-level directory is the root directory of the source code.

1. Confirm the ORC source code directory.

    ```shell
    ls /opt/omni/src/orc        # CMakeLists.txt  c++  cmake_modules  proto  tools ...
    ```

2. Set the environment variables required for building ORC.

    >![](public_sys-resources/icon-notice.gif) **NOTICE:**
    >All `*_HOME` environment variables must be set. Otherwise, ORC downloads and builds lz4, zstd, zlib, and snappy again through ExternalProject, and the snappy build fails.

    ```shell
    export PROTOBUF_HOME=/opt/Gluten
    export ZSTD_HOME=/usr/local
    export LZ4_HOME=/usr/local
    export ZLIB_HOME=/usr/local
    export ZLIB_NG_HOME=/usr/local
    export SNAPPY_HOME=/usr/local
    export PATH=/opt/Gluten/bin:$PATH
    ```

3. Build and install ORC.

    ```shell
    cd /opt/omni/src/orc
    rm -rf build && mkdir build && cd build

    cmake .. \
      -DCMAKE_BUILD_TYPE=Release \
      -DCMAKE_INSTALL_PREFIX=/usr/local \
      -DCMAKE_PREFIX_PATH="/opt/Gluten;/usr/local" \
      -DBUILD_JAVA=OFF \
      -DANALYZE_JAVA=OFF \
      -DBUILD_LIBHDFSPP=OFF \
      -DBUILD_CPP_TESTS=OFF \
      -DBUILD_TOOLS=ON \
      -DBUILD_POSITION_INDEPENDENT_LIB=ON

    make -j32
    make install
    ldconfig
    ```

### Generate liborc.so<a name="generate_liborc"></a>

In the `CMakeLists.txt` of ORC 1.7.4, the library is fixed as `add_library(orc STATIC ...)`, so only the static library `liborc.a` is generated. `reader.so` requires a shared library when linking with `-lorc`; otherwise, dependencies such as protobuf, zlib, lz4, zstd, and snappy cannot be passed.

1. Generate the shared library `liborc.so` from the static library.

    ```shell
    ORC_A=/opt/omni/src/orc/build/c++/src/liborc.a

    g++ -shared -o /usr/local/lib/liborc.so.1.7.4 \
      -Wl,--whole-archive "$ORC_A" -Wl,--no-whole-archive \
      -lprotobuf -lz -llz4 -lzstd -lsnappy -lpthread \
      -L/opt/Gluten/lib -L/usr/local/lib64 -L/usr/local/lib

    ln -sf liborc.so.1.7.4 /usr/local/lib/liborc.so

    echo "/opt/Gluten/lib" > /etc/ld.so.conf.d/gluten.conf
    ldconfig
    ```

2. Verify that all dependencies of `liborc.so` are resolved.

    ```shell
    ldd /usr/local/lib/liborc.so.1.7.4 | grep 'not found'   # No output indicates that the dependencies are resolved.
    ```

## Build OmniOperator<a name="build_omnioperator"></a>

### Obtain the Source Code and Switch Branches<a name="obtain_omnioperator_source"></a>

1. Clone the OmniOperator source code and switch to the internet_commercial branch.

    ```shell
    cd /opt/omni/src
    rm -rf OmniOperator
    git clone https://gitcode.com/openeuler/OmniOperator.git
    cd OmniOperator

    git fetch https://gitcode.com/openeuler/OmniOperator.git \
      internet_commercial:refs/remotes/openeuler/internet_commercial
    git checkout -B internet_commercial refs/remotes/openeuler/internet_commercial
    ```

2. Create the `clang++-15` symbolic link.

    >![](public_sys-resources/icon-note.gif) **NOTE:**
    >The top-level `CMakeLists.txt` uses `clang++-15` for compilation. openEuler does not provide this command by default, so create a symbolic link.

    ```shell
    ln -sf /usr/lib/llvm-15/bin/clang++ /usr/local/bin/clang++-15
    ```

### Build the Native Libraries<a name="build_native"></a>

1. Set the environment variables required for compilation.

    ```shell
    source /opt/omni/env.sh

    cd /opt/omni/src/OmniOperator
    export OMNI_HOME=/opt/omni/install_internet
    export OMNI_COMPILER_THREAD_COUNT=32

    # The compute module references the Protobuf io header but does not propagate the include path.
    # The ORC header uses relative includes internally, so the orc directory itself must be added to the search path.
    export CXXFLAGS="-I/opt/Gluten/include -I/opt/Adaptor/include \
      -I/usr/local/include -I/usr/local/include/orc"

    # libhdfs exists only in /opt/Adaptor/lib.
    export LIBRARY_PATH="/opt/Adaptor/lib:/opt/Gluten/lib:/usr/local/lib64:/usr/local/lib"
    ```

2. Perform the build.

    ```shell
    bash build_scripts/build.sh release:java --exclude-test
    ```

    >![](public_sys-resources/icon-notice.gif) **NOTICE:**
    >- Setting `OMNI_COMPILER_THREAD_COUNT` to 256 is not recommended. When openEuler GCC 12 compiles SVE code with high concurrency, the error `internal compiler error: Bus error ... during RTL pass: cprop` may be triggered. The build is stable when the value is set to 32.
    >- `build.sh` deletes `build/CMakeCache.txt` each time it is executed, so the preceding environment variables take effect again in every build.
    >- `build.sh release:java` builds only the native part and does not generate JAR packages. For details about how to generate JAR packages, see [Generate JAR Packages](#build_jar).

### Generate JAR Packages<a name="build_jar"></a>

1. Build the Java binding.

    ```shell
    cd /opt/omni/src/OmniOperator/bindings/java
    mvn -B clean install -Domni.home="$OMNI_HOME" -DskipTests
    ```

2. Build the UDF.

    ```shell
    cd /opt/omni/src/OmniOperator/core/src/udf/java
    mvn -B clean install -DskipTests
    ```

>![](public_sys-resources/icon-note.gif) **NOTE:**
>Maven needs to access remote repositories. The Huawei Cloud mirror has been configured in the [Install Maven](#install_maven) section. In a completely offline environment, preheat the `~/.m2/repository` directory or copy the entire directory in advance.

## Verify the Installation<a name="verify_installation"></a>

### Check the Artifacts<a name="check_artifacts"></a>

1. Check the library files and Java binding artifacts.

    ```shell
    ls -lh /opt/omni/install_internet/lib
    ls -lh /opt/omni/install_internet/java-binding
    ```

    The artifact directory structure is as follows.

    ```text
    /opt/omni/install_internet/
    ├── lib/
    │   ├── libboostkit-omniop-vector-2.2.0-aarch64.so
    │   ├── libboostkit-omniop-codegen-2.2.0-aarch64.so
    │   ├── libboostkit-omniop-operator-2.2.0-aarch64.so
    │   ├── libboostkit-omniop-reader-2.2.0-aarch64.so
    │   └── include/
    └── java-binding/
        └── libboostkit-omniop-java-binding-2.2.0-aarch64.so
    ```

2. Check the JAR package artifacts.

    ```text
    bindings/java/target/boostkit-omniop-bindings-2.2.0-aarch64.jar
    core/src/udf/java/target/boostkit-omniop-udf-2.2.0-aarch64.jar
    ```

### Check Dynamic Library Dependencies<a name="check_dynamic_dependencies"></a>

1. Check the dynamic library dependencies of the `reader` module.

    ```shell
    ldd /opt/omni/install_internet/lib/libboostkit-omniop-reader-2.2.0-aarch64.so | grep 'not found'
    ```

    No output indicates that the dependencies are resolved.

2. Confirm that the key dependencies are resolved correctly.

    ```text
    libarrow_dataset.so.1100 -> /usr/local/lib64/libarrow_dataset.so.1100
    libparquet.so.1100       -> /usr/local/lib64/libparquet.so.1100
    liborc.so                -> /usr/local/lib/liborc.so
    libhdfs.so.0.0.0         -> /opt/Adaptor/lib/libhdfs.so.0.0.0
    libjvm.so                -> /usr/lib/jvm/java-1.8.0-openjdk/jre/lib/aarch64/server/libjvm.so
    libprotobuf.so.32        -> /opt/Gluten/lib/libprotobuf.so.32
    ```

### Configure Runtime Library Paths<a name="configure_runtime_path"></a>

1. Write the library directories required at runtime to the dynamic library configuration file.

    ```shell
    cat > /etc/ld.so.conf.d/omni-adaptor.conf <<'EOF'
    /opt/Adaptor/lib
    /usr/lib/jvm/java-1.8.0-openjdk/jre/lib/aarch64/server
    EOF
    ldconfig
    ```

    >![](public_sys-resources/icon-note.gif) **NOTE:**
    >`/opt/Gluten/lib` has been written to `/etc/ld.so.conf.d/gluten.conf` in the [Generate liborc.so](#generate_liborc) section. `/usr/local/lib`, `/usr/local/lib64`, and `/usr/lib/llvm-15/lib` are configured in the [Configure the Dynamic Library Search Path](#configure_ld_so) and [Install LLVM/Clang 15.0.4](#install_llvm) sections, respectively.

2. Set the runtime environment variables.

    ```shell
    export LD_LIBRARY_PATH=/opt/omni/install_internet/lib:/opt/omni/install_internet/java-binding:$LD_LIBRARY_PATH
    ```

## Deployment Notes<a name="deployment_notes"></a>

- `/opt/omni/install_internet/lib` is the library directory used for deployment. `libboundscheck.so`, `liborc.so`, `libfolly.so`, and `libarrow*` are external dependencies and are not included in this directory. Install them on the target machine or add them to `LD_LIBRARY_PATH`.
- If the target machine does not have the `/opt/Adaptor` directory, copy `libhdfs.so*`, `libhdfspp.so*`, and `include/` to the target machine.
- The version information is subject to the verification output in the [Verify the Installation](#verify_installation) section.

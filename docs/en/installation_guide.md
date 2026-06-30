# Installation Guide<a name="ZH-CN_TOPIC_0000002515622552"></a>

## Installation Overview<a name="ZH-CN_TOPIC_0000002515782482"></a>

### Network Planning<a name="ZH-CN_TOPIC_0000002515782488"></a>

A storage-compute coupled networking architecture is recommended for the subfeatures. Storage nodes and compute nodes are shared to maximize the computing acceleration effect in big data scenarios.

The coupled storage and compute network of OmniOperator consists of four servers, which are one management node and three compute nodes. In the following, we will be using HDFS on the storage nodes for illustration:

- The management node is server1 for managing tasks.
- The compute nodes are agent1, agent2, and agent3, which are used to run OmniOperator.

A server can function as a management node and a compute node at the same time. In single-node mode, operations performed on the management node or compute node mentioned in the following sections are performed on the same node. [**Figure 1** Networking diagram](#networking-diagram) shows the networking diagram.

**Figure 1** Networking diagram<a name="fig734218245342"></a><a id="networking-diagram"></a><br>
![](figures/networking-diagram.png "Networking diagram")

### Environment Requirements<a name="ZH-CN_TOPIC_0000002515622544"></a>

Before installing OmniOperator, prepare the hardware and software environments to facilitate subsequent installation operations.

**Hardware Requirements<a name="section197116445713"></a>**

[**Table 1** Hardware requirements](#hardware_requirements) lists the hardware requirements for each node in the cluster.

**Table 1** Hardware requirements<a id="hardware_requirements"></a>

|Item|Management/Compute/Storage Node|
|--|--|
|Processor|Kunpeng 920<br>Kunpeng 950<br>![](./public_sys-resources/icon-caution.gif)**NOTICE**<br>OmniOperator can be enabled for Gluten only on servers that support the SVE instruction set. You can run `cat /proc/cpuinfo \| grep sve \| head -n 1` to check whether the SVE instruction set is supported. If any command output is displayed, the SVE instruction set is supported.|
|Memory|384 GB (12 x 32 GB)|
|Memory frequency|2,666 MHz|
|Network|Service network: 10GE<br>Management network: 1GE|
|Drives|System drive: 1 x RAID 0 (1 x 1.2 TB SAS HDD)<br> Data drive: 12 x RAID 0 (12 x 8 TB SATA HDD)|
|RAID controller card|LSI SAS3508|

**OS and Software Requirements<a name="section112321019581"></a>**

[**Table 2** OS and software requirements](#operating_system_and_software_requirements) lists the OS and software requirements of each node in the cluster.

**Table 2** OS and software requirements<a id="operating_system_and_software_requirements"></a>

|Item| Version                                                      | Description                                                                                                |Management Node (Server)|Compute/Storage Node|
|--|----------------------------------------------------------|----------------------------------------------------------------------------------------------------|--|--|
|OS| CentOS 7.9<br>openEuler 20.03 LTS SP1<br>openEuler 22.03 LTS SP1<br>openEuler 24.03 LTS SP1 | Later patch versions such as openEuler 20.03 LTS SP3, openEuler 22.03 LTS SP3, and openEuler 24.03 LTS SP3 are also supported.             |√|√|
|JDK| [BiSheng JDK 1.8 (BiSheng JDK 1.8.0_342)](https://mirror.iscas.ac.cn/kunpeng/archive/compiler/bisheng_jdk/bisheng-jdk-8u342-linux-aarch64.tar.gz)<br>[BiSheng JDK 17](https://mirrors.huaweicloud.com/kunpeng/archive/compiler/bisheng_jdk/bisheng-jdk-17.0.18-b13-linux-aarch64.tar.gz) | openEuler 22.03 LTS SP1 is incompatible with BiSheng JDK 1.8.0_262, which must be replaced with [BiSheng JDK 1.8.0_342](https://mirror.iscas.ac.cn/kunpeng/archive/compiler/bisheng_jdk/bisheng-jdk-8u342-linux-aarch64.tar.gz). For details about how to install the BiSheng JDK, see [BiSheng JDK 8 Installation Guide](https://gitee.com/openeuler/bishengjdk-8/wikis/%E4%B8%AD%E6%96%87%E6%96%87%E6%A1%A3/%E6%AF%95%E6%98%87JDK%208%20%E5%AE%89%E8%A3%85%E6%8C%87%E5%8D%97).|√|√|
|Hadoop| [3.2.0](https://archive.apache.org/dist/hadoop/common/hadoop-3.2.0/hadoop-3.2.0.tar.gz)                                                | See [Hadoop Cluster Deployment (CentOS 7.6 & openEuler 20.03)](https://www.hikunpeng.com/document/detail/en/kunpengbds/ecosystemEnable/Hadoop/kunpenghadoop_04_0001.html).                                             |√|√|
|Spark| [3.1.1](https://archive.apache.org/dist/spark/spark-3.1.1/spark-3.1.1-bin-hadoop3.2.tgz) [3.3.1](https://archive.apache.org/dist/spark/spark-3.3.1/spark-3.3.1-bin-hadoop3.tgz) [3.4.3](https://archive.apache.org/dist/spark/spark-3.4.3/spark-3.4.3-bin-hadoop3.tgz) [3.5.2](https://archive.apache.org/dist/spark/spark-3.5.2/spark-3.5.2-bin-hadoop3.tgz)                  | See [Spark Cluster Deployment (CentOS 7.6 & openEuler 20.03)](https://www.hikunpeng.com/document/detail/en/kunpengbds/ecosystemEnable/Spark/kunpengspark_04_0001.html).                                              |√|-|
|Hive| [3.1.0](https://archive.apache.org/dist/hive/hive-3.1.0/apache-hive-3.1.0-bin.tar.gz)                                                | See [Hive Cluster Deployment (CentOS 7.6 & openEuler 20.03)](https://www.hikunpeng.com/document/detail/en/kunpengbds/ecosystemEnable/Hive/kunpenghive_04_0001.html).                                                |√|-|
|Python| [3.10.2 or later](https://www.python.org/ftp/python/)                                           | No special requirements.                                                                                            |√|√|

>![](public_sys-resources/icon-note.gif) **NOTE:**
>
>- √: indicates that the item is required on the node.
>- -: indicates that the item is not required on the node.
>- If the preceding third-party software has vulnerabilities, fix the vulnerabilities based on official instructions.
>- The preceding component versions may be different from those in the *Deployment Guide*. The *Deployment Guide* is for reference only.
>- SparkExtension can run on CentOS 7.9, openEuler 20.03, and openEuler 22.03.
>- Gluten can run on openEuler 22.03 and openEuler 24.03.

**Obtaining Software Installation Packages<a name="section189181357102011"></a>**

[**Table 3** OmniOperator software packages](#omnioperator_software_obtains_columns) describes the software packages required for installing OmniOperator. In subsequent operations, install the software based on the operation guide.

>![](public_sys-resources/icon-note.gif) **NOTE:**
>
>Use on Spark:
>
>- SparkExtension requires installing the software packages numbered 1, 2 (select the SparkExtension version according to the Spark version), and 5.
>- Gluten requires installing the software package numbered 4.
>
>Use on Hive:
>
>- HiveExtension requires installing the software packages numbered 1, 3, and 5.

**Table 3** OmniOperator software packages<a id="omnioperator_software_obtains_columns"></a>

<table border="1" cellpadding="6" cellspacing="0">
  <thead>
    <tr>
      <th>No.</th>
      <th>Name</th>
      <th>Package Name</th>
      <th>Release Type</th>
      <th>Description</th>
      <th>How to Obtain</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td rowspan="1">1</td>
      <td>OmniRuntime package</td>
      <td>BoostKit-omniruntime_1.9.0.zip</td>
      <td>Closed source</td>
      <td>OmniRuntime package (BoostKit-omniruntime_1.9.0.zip). Extract the package to obtain the OmniOperator software package BoostKit-omniop_2.0.0.zip.</td>
      <td>Kunpeng community: <a href="https://kunpeng-repo.obs.cn-north-4.myhuaweicloud.com/Kunpeng%20BoostKit/Kunpeng%20BoostKit%2025.3.0/BoostKit-omniruntime_1.9.0.zip">Link</a><br>Before using the software package, read <a href="https://www.hikunpeng.com/en/legal/developer/boostkit/software/protocol">Kunpeng BoostKit User License Agreement 2.0</a>. Continued use indicates your acceptance of the terms and conditions.</td>
    </tr>
    <tr>
      <td rowspan="4">2</td>
      <td rowspan="4">SparkExtension</td>
      <td>boostkit-omniop-spark-3.1.1-2.0.0-aarch64.zip</td>
      <td>Open source</td>
      <td>Spark extension package for the OmniOperator computing base.</td>
      <td><a href="https://gitcode.com/boostkit/boostkit-bigdata/releases/download/Kunpeng-BoostKit-25.3.0-OmniOperator-release-311/boostkit-omniop-spark-3.1.1-2.0.0-aarch64.zip">Link</a></td>
    </tr>
    <tr>      
      <td>boostkit-omniop-spark-3.3.1-2.0.0-aarch64.zip</td>
      <td>Open source</td>
      <td>Spark extension package for the OmniOperator computing base.</td>
      <td><a href="https://gitcode.com/boostkit/boostkit-bigdata/releases/download/Kunpeng-BoostKit-25.3.0-OmniOperator-release/boostkit-omniop-spark-3.3.1-2.0.0-aarch64.zip">Link</a></td>
    </tr>
    <tr>      
      <td>boostkit-omniop-spark-3.4.3-2.0.0-aarch64.zip</td>
      <td>Open source</td>
      <td>Spark extension package for the OmniOperator computing base.</td>
      <td><a href="https://gitcode.com/boostkit/boostkit-bigdata/releases/download/Kunpeng-BoostKit-25.3.0-OmniOperator-release/boostkit-omniop-spark-3.4.3-2.0.0-aarch64.zip">Link</a></td>
    </tr>
    <tr>      
      <td>boostkit-omniop-spark-3.5.2-2.0.0-aarch64.zip</td>
      <td>Open source</td>
      <td>Spark extension package for the OmniOperator computing base.</td>
      <td><a href="https://gitcode.com/boostkit/boostkit-bigdata/releases/download/Kunpeng-BoostKit-25.3.0-OmniOperator-release/boostkit-omniop-spark-3.5.2-2.0.0-aarch64.zip">Link</a></td>
    </tr>
    <tr>
      <td rowspan="1">3</td>
      <td>HiveExtension</td>
      <td>boostkit-omniop-hive-3.1.0-2.0.0-aarch64.zip</td>
      <td>Open source</td>
      <td>Hive extension package for the OmniOperator computing base.</td>
      <td><a href="https://gitcode.com/boostkit/boostkit-bigdata/releases/download/Kunpeng-BoostKit-25.3.0-OmniOperator-release-hive/boostkit-omniop-hive-3.1.0-2.0.0-aarch64.zip">Link</a></td>
    </tr>
    <tr>
      <td rowspan="4">4</td>
      <td rowspan="4">Gluten</td>
      <td>Boostkit-omniruntime-gluten-2.2.0.zip</td>
      <td>Open source</td>
      <td>OmniOperator software installation package (adapted to Gluten and Spark 3.5.2)</td>
      <td><a href="https://atomgit.com/openeuler/OmniOperator/releases/download/26.1.0-OmniOperator-2.2.0-release/BoostKit-omniruntime-gluten-2.2.0.zip">Link</a></td>
    </tr>
    <tr>      
      <td>Boostkit-omniruntime-gluten-2.0.0.zip</td>
      <td>Open source</td>
      <td>OmniOperator software installation package (adapted to Gluten and Spark 3.4.3)</td>
      <td><a href="https://gitcode.com/openeuler/OmniOperator/releases/download/26.1.0-OmniOperator-2.2.0-release/Dependency_library_Gluten.zip">Link</a></td>
    </tr>
    <tr>      
      <td>Boostkit-omniruntime-gluten-1.0.0.zip</td>
      <td>Open source</td>
      <td>OmniOperator software installation package (adapted to Gluten and Spark 3.3.1)</td>
      <td><a href="https://gitcode.com/openeuler/OmniOperator/releases/download/25.3.0-OmniOperator-1.0.0-release/BoostKit-omniruntime-gluten-1.0.0.zip">Link</a></td>
    </tr>
    <tr>      
      <td>Dependency_library_Gluten.zip</td>
      <td>Open source</td>
      <td>Library file on which Gluten depends.</td>
      <td><a href="https://gitcode.com/openeuler/OmniOperator/releases/download/26.1.0-OmniOperator-2.2.0-release/Dependency_library_Gluten.zip">Link</a></td>
    </tr>
    <tr>
      <td rowspan="1">5</td>
      <td>Dependency_library</td>
      <td>Dependency_library_centos.zip<br>Dependency_library_openeuler20.03.zip<br>Dependency_library_openeuler22.03.zip</td>
      <td>Open source</td>
      <td>Library file on which OmniOperator depends. Select the dependency package that matches your OS type.</td>
      <td><a href="https://gitcode.com/boostkit/boostkit-bigdata/releases/download/Kunpeng-BoostKit-25.3.0-OmniOperator-release/Dependency_library_centos.zip">CentOS dependencies</a><br><a href="https://gitcode.com/boostkit/boostkit-bigdata/releases/download/Kunpeng-BoostKit-25.3.0-OmniOperator-release/Dependency_library_openeuler20.03.zip">openEuler 20.03 dependencies</a><br><a href="https://gitcode.com/boostkit/boostkit-bigdata/releases/download/Kunpeng-BoostKit-25.3.0-OmniOperator-release/Dependency_library_openeuler22.03.zip">openEuler 22.03 dependencies</a></td>
    </tr>
  </tbody>
</table>  
                                                           
**Software Package Integrity Check<a name="section16501429204018"></a>**

After downloading a software installation package from the Kunpeng community, verify its integrity to ensure it matches the original package on the website.

Verification method:

1. Obtain the software digital certificate and installation package.
2. Obtain the [verification tool and method](https://support.huawei.com/enterprise/en/tool/pgp-verify-TL1000000054).
3. Verify the package integrity by following the procedure described in the *OpenPGP Signature Verification Guide* obtained in the previous step.

## Feature Installation<a name="ZH-CN_TOPIC_0000002515782478"></a>

### Node Requirements<a name="ZH-CN_TOPIC_0000002515782464"></a>

This section describes the requirements for installing dependency packages and configuring environment variables on each node before installing OmniOperator.

- If you choose to compile the source code, install GCC/G++, Autoconf, and CMake on each node first. For details about the version requirements, see [**Table 1** Software to be configured before source code compilation](#Software-to-be-configured-before-source-code-compilation).

    >![](public_sys-resources/icon-note.gif) **NOTE:**
    >- LLVM and Jemalloc can run properly only after being compiled on the OS. If you want to run them on CentOS, compile them on CentOS. If you want to run them on openEuler 20.03 LTS SP1 or openEuler 22.03 LTS SP1, compile them on openEuler 20.03 LTS SP1 or openEuler 22.03 LTS SP1.
    >- Gluten depends on the ABSL library and can run properly only after it is compiled and installed on the current OS (openEuler 22.03 SP1).

    **Table 1** Software to be configured before source code compilation<a id="Software-to-be-configured-before-source-code-compilation"></a>

<table border="1" cellpadding="6" cellspacing="0">
  <thead>
    <tr>
      <th>Name</th>
      <th>Version</th>
      <th>Link</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td rowspan="3">GCC/G++</td>
      <td>openEuler 20.03: 7.3.0</td>
      <td><a href="https://mirrors.tuna.tsinghua.edu.cn/gnu/gcc/gcc-7.3.0/gcc-7.3.0.tar.gz" target="_blank">Link</a></td>
    </tr>
    <tr>
      <td>openEuler 22.03: 10.3.0</td>
      <td><a href="https://mirrors.tuna.tsinghua.edu.cn/gnu/gcc/gcc-10.3.0/gcc-10.3.0.tar.gz" target="_blank">Link</a></td>
    </tr>
    <tr>
      <td>openEuler 24.03: 12.3.0</td>
      <td><a href="https://mirrors.tuna.tsinghua.edu.cn/gnu/gcc/gcc-12.3.0/gcc-12.3.0.tar.gz" target="_blank">Link</a></td>
    </tr>
    <tr>
      <td>Autoconf</td>
      <td>2.69</td>
      <td><a href="https://ftp.gnu.org/gnu/autoconf/autoconf-2.69.tar.gz" target="_blank">Link</a></td>
    </tr>
    <tr>
      <td>CMake</td>
      <td>3.20.5</td>
      <td><a href="https://github.com/Kitware/CMake/releases/download/v3.20.5/cmake-3.20.5-linux-aarch64.tar.gz" target="_blank">Link</a></td>
    </tr>
  </tbody>
</table>

1. Compile and install GCC/G++. The following uses version 7.3.0 as an example.
    1. Check whether the GCC/G++ version is the target version.

        ```shell
        gcc --version 
        g++ --version        
        ```    

    2. Compile and install GCC/G++.

      ```shell
        # Extract the installation package and go to the gcc-7.3.0 directory.
        tar -zxvf gcc-7.3.0.tar.gz
        cd gcc-7.3.0
        # Perform compilation and installation.
        mkdir build && cd build
        ../configure --prefix=/usr/local/gcc-7.3.0 --enable-languages=c,c++ --disable-multilib 
        make -j$(nproc)
        make install
        # Set environment variables.
        echo 'export PATH=/usr/local/gcc-7.3.0/bin:$PATH' >> /etc/profile
        echo 'export LD_LIBRARY_PATH=/usr/local/gcc-7.3.0/lib64:$LD_LIBRARY_PATH' >> /etc/profile
        source /etc/profile
        # Check whether the installation is successful.
        gcc --version
        g++ --version
      ```   

2. Compile and install Autoconf.

    1. Check whether the Autoconf version is the target version.
        
        ```shell
        autoconf --version
        ```  

    2. Compile and install Autoconf.

        ```shell
        yum install -y m4
        # Extract the installation package and go to the autoconf-2.69 directory.
        tar -zxvf autoconf-2.69.tar.gz
        cd autoconf-2.69
        # Perform compilation and installation.
        ./configure --prefix=/usr/local/autoconf-2.69
        make -j$(nproc)
        make install
        # Set environment variables.
        echo 'export PATH=/usr/local/autoconf-2.69/bin:$PATH' >> /etc/profile
        source /etc/profile
        # Check whether the installation is successful.
        autoconf --version
        ```
        
3. Install CMake.
    1. Check whether the CMake version is the target version.

        ```shell 
        cmake --version
        ```

    2. Install CMake.

        ```shell 
        # Extract the installation package to any directory (/opt in this example).
        tar -zxvf cmake-3.20.5-linux-aarch64.tar.gz -C /opt
        # Set environment variables.
        echo 'export PATH=/opt/cmake-3.20.5-linux-aarch64/bin:$PATH' >> /etc/profile
        source /etc/profile
        # Check whether the installation is successful.
        cmake --version
        ```

- Before installing OmniOperator, deploy required components in the cluster based on [**Table 2** OS and software requirements](#operating_system_and_software_requirements).
- Before configuring environment variables, check whether the environment variable `LD_LIBRARY_PATH` exists in your environment. If it does not exist, you do not need to add `$LD_LIBRARY_PATH` during configuration, so as to prevent the current directory `cwd` from being introduced to the dynamic library search path. Otherwise, security problems may occur. All environment variable export operations in this document comply with this principle. Take `LD_LIBRARY_PATH` as an example. If `LD_LIBRARY_PATH` already exists in your environment, use `export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/xxx`. Otherwise, use `export LD_LIBRARY_PATH=/xxx`.

### Installing Dependencies<a name="ZH-CN_TOPIC_0000002515782474"></a>

When installing OmniOperator locally, install the LLVM and jemalloc dependency packages on the management node to combine OmniOperator and Spark.

In the Spark on Yarn scenario, use the `--archives` parameter of Spark to simplify the deployment. You can install the dependencies either by downloading precompiled SO files or by compiling source code. The precompiled SO file can be downloaded and installed quickly, and is suitable for most scenarios. In contrast, compiling and installing from source code is slower, but may be required for certain compliance scenarios.

**Installing Dependencies (Downloading and Installing Precompiled .so Files, Using SparkExtension)<a name="section54515015398"></a>**

**Installing LLVM and jemalloc**

>![](public_sys-resources/icon-note.gif) **NOTE:**
>
>- Select the dependency package based on your OS type. The following uses openEuler 22.03 as an example and the dependency package is `Dependency_library_openeuler22.03.zip`.
>- The `/opt/omni-operator` and `/opt/omni-operator/lib` directories can be customized.

1. Create the `/opt/omni-operator/` directory on the management node as the root directory for installing OmniOperator. Then go to the directory.

    ```shell
    mkdir /opt/omni-operator
    cd /opt/omni-operator
    ```
    
2. Obtain `Dependency_library_openeuler22.03.zip` from [**Table 3** OmniOperator software packages](#omnioperator_software_obtains_columns), upload it to the `/opt/omni-operator/` directory, and extract it.

    ```shell
    unzip Dependency_library_openeuler22.03.zip
    ```

3. Create the `/opt/omni-operator/lib` directory and copy the `libLLVM-15.so` and `libjemalloc.so.2` files in `Dependency_library_openeuler` to the `/opt/omni-operator/lib` directory.

    >![](public_sys-resources/icon-notice.gif) **NOTICE:**
    >If LLVM and jemalloc have been installed in the environment, delete the existing `libLLVM-15.so` and `libjemalloc.so.2` files before running the copy command.
    >
    >```shell
    >rm -rf /opt/omni-operator/lib/libjemalloc.so.2
    >rm -rf /opt/omni-operator/lib/libLLVM-15.so
    >```

    ```shell
    cd /opt/omni-operator
    mkdir lib
    cp /opt/omni-operator/Dependency_library_openeuler22.03/libjemalloc.so.2 /opt/omni-operator/lib
    cp /opt/omni-operator/Dependency_library_openeuler22.03/libLLVM-15.so /opt/omni-operator/lib
    ```

**Installing Dependencies (Downloading and Installing Precompiled .so Files, Using Gluten)<a name="section11238405227"></a>**

**Installing LLVM and jemalloc**

>![](public_sys-resources/icon-note.gif) **NOTE:**
>
>- The precompiled SO files of the ABSL library are not provided for Gluten. Therefore, you need to manually compile and install the SO files.
>- The `/opt/omni-operator` and `/opt/omni-operator/lib` directories can be customized.

1. Create the `/opt/omni-operator/` directory on the management node as the root directory for installing OmniOperator. Then go to the directory.

    ```shell
    mkdir /opt/omni-operator
    cd /opt/omni-operator
    rm Dependency_library_Gluten.zip -rf
    ```

2. Obtain `Dependency_library_Gluten.zip` from [**Table 3** OmniOperator software packages](#omnioperator_software_obtains_columns), upload it to the `/opt/omni-operator/` directory, and extract it.

    ```shell
    unzip Dependency_library_Gluten.zip
    ```

3. Create the `/opt/omni-operator/lib` directory and copy the `libLLVM-15.so` and `libjemalloc.so.2` files in `Dependency_library_Gluten` to the `/opt/omni-operator/lib` directory.

    >![](public_sys-resources/icon-notice.gif) **NOTICE:**
    >If LLVM and jemalloc have been installed in the environment, delete the existing libLLVM-15.so and libjemalloc.so.2 files before running the copy command.
    >
    >```shell
    >rm -rf /opt/omni-operator/lib/libjemalloc.so.2
    >rm -rf /opt/omni-operator/lib/libLLVM-15.so
    >```

    ```shell
    cd /opt/omni-operator
    mkdir lib
    cp /opt/omni-operator/Dependency_library_Gluten/libjemalloc.so.2 /opt/omni-operator/lib
    cp /opt/omni-operator/Dependency_library_Gluten/libLLVM-15.so /opt/omni-operator/lib
    ```

4. Compile and install ABSL. For details, see steps [1](#li17569353267) to [3](#li1220195592616).

**Installing Dependencies (Compiling Source Code, Using SparkExtension and Gluten)<a name="section3182322111816"></a>**

**Installing LLVM**

>![](public_sys-resources/icon-note.gif) **NOTE:**
>The `/opt/omni-operator`, `/opt/omni-operator/llvm`, and `/opt/omni-operator/lib` directories can be customized.

1. Download [llvm-project-llvmorg-15.0.4.tar.gz](https://github.com/llvm/llvm-project/archive/refs/tags/llvmorg-15.0.4.tar.gz), create the `/opt/omni-operator` directory on the management node as the root directory for installing OmniOperator, go to the directory, and upload the package to `/opt/omni-operator`.

    ```shell
    mkdir /opt/omni-operator
    cd /opt/omni-operator
    tar zxvf llvm-project-llvmorg-15.0.4.tar.gz
    mv llvm-project-llvmorg-15.0.4 llvm
    cd llvm
    mkdir build
    ```

2. Go to the `build` directory, and compile and install LLVM.

    ```shell
    cd ./build
    cmake -DCMAKE_INSTALL_PREFIX=/opt/omni-operator/llvm -DCMAKE_BUILD_TYPE=Release -DLLVM_BUILD_LLVM_DYLIB=true -DLLVM_ENABLE_PROJECTS="clang" -G "Unix Makefiles" ../llvm
    make -j4
    make install
    ```

3. Create the `lib` directory in `/opt/omni-operator` and copy `/opt/omni-operator/llvm/lib/libLLVM-15.so` to `/opt/omni-operator/lib`.

    ```shell
    mkdir /opt/omni-operator/lib
    cp /opt/omni-operator/llvm/lib/libLLVM-15.so /opt/omni-operator/lib/
    ```

**Installing jemalloc**

1. Download [jemalloc-5.3.0.tar.gz](https://github.com/jemalloc/jemalloc/archive/refs/tags/5.3.0.tar.gz) and upload it to the management node.

    ```shell
    cd /opt/omni-operator/
    tar zxvf jemalloc-5.3.0.tar.gz
    mv jemalloc-5.3.0 jemalloc
    ```

    >![](public_sys-resources/icon-note.gif) **NOTE:**
    >The `/opt/omni-operator/jemalloc` directory can be customized.

2. Go to the `jemalloc` directory, run the script, and install the generated file.

    ```shell
    cd jemalloc
    ./autogen.sh --disable-initial-exec-tls
    make -j2
    ```

3. Copy `/opt/omni-operator/jemalloc/lib/libjemalloc.so.2` to the `/opt/omni-operator/lib` directory.

    ```shell
    cp /opt/omni-operator/jemalloc/lib/libjemalloc.so.2 /opt/omni-operator/lib/
    ```

**Installing ABSL** (required only when ABSL is enabled for Gluten)

1. <a name="li17569353267"></a>Download the ABSL source code to the management node.

    ```shell
    git clone https://github.com/abseil/abseil-cpp.git
    cd abseil-cpp/
    git checkout tags/20250127.0
    ```

2. Compile the ABSL source code.

    ```shell
    mkdir build && cd build
    cmake ..   -DCMAKE_CXX_STANDARD=17   -DCMAKE_CXX_STANDARD_REQUIRED=ON   -DABSL_PROPAGATE_CXX_STD=ON -DBUILD_SHARED_LIBS=ON
    make -j32
    make install
    ```

3. <a name="li1220195592616"></a>Copy the compiled ABSL library to `/opt/omni-operator/lib`.

    ```shell
    cp /usr/local/lib64/libabsl_* /opt/omni-operator/lib
    ```

### Installing OmniOperator<a name="ZH-CN_TOPIC_0000002515622558"></a>

Install OmniOperator on the management and compute nodes and set environment variables. If OmniOperator is enabled for Gluten, skip this section.

>![](public_sys-resources/icon-note.gif) **NOTE:**
>
>- `BoostKit-omniop_2.0.0.zip` can be obtained by extracting `BoostKit-omniruntime_1.9.0.zip`. The `BoostKit-omniop_2.0.0.zip` package contains the `boostkit-omniop-operator-2.0.0-aarch64-openeuler.tar.gz` and `boostkit-omniop-operator-2.0.0-aarch64-centos.tar.gz` packages, which are used for openEuler and CentOS, respectively. The following uses openEuler as an example.
>- To install OmniOperator on CentOS, replace `boostkit-omniop-operator-2.0.0-aarch64-openeuler.tar.gz` in the following commands with `boostkit-omniop-operator-2.0.0-aarch64-centos.tar.gz`.

1. Upload the packages in [**Table 3** OmniOperator software packages](#omnioperator_software_obtains_columns) to the `/opt/omni-operator/` directory on the management and compute nodes.
2. Go to the `/opt/omni-operator/` directory and extract the packages.

    ```shell
    cd /opt/omni-operator/
    unzip BoostKit-omniruntime_1.9.0.zip
    unzip BoostKit-omniop_2.0.0.zip
    tar -zxvf boostkit-omniop-operator-2.0.0-aarch64-openeuler.tar.gz
    ```

3. Copy OmniOperator files to the `/opt/omni-operator/lib` directory and set the permissions on the software packages in the directory to `550`.

    ```shell
    cd /opt/omni-operator/boostkit-omniop-operator-2.0.0-aarch64
    cp -r include libboostkit* boostkit-omniop* libsecurec.so /opt/omni-operator/lib/
    chmod -R 550 /opt/omni-operator/lib/*
    ```

4. Create the `conf` folder in the `/opt/omni-operator` directory and set the folder permissions to `750`.

    ```shell
    cd /opt/omni-operator
    mkdir conf
    chmod 750 /opt/omni-operator/conf
    ```

5. Create the `omni.conf` file in the `conf` folder and change the file permissions to `640`, which is required to set OmniOperator configuration items.

    ```shell
    cd conf
    touch omni.conf
    chmod 640 omni.conf
    ```

6. Delete redundant files from `/opt/omni-operator`.

    ```shell
    mkdir -p /opt/omni-operator-bak
    mv /opt/omni-operator/lib /opt/omni-operator-bak
    mv /opt/omni-operator/conf /opt/omni-operator-bak
    ls /opt/omni-operator
    rm -rf /opt/omni-operator
    cd /opt
    mv omni-operator-bak omni-operator
    ```

### (Optional) Installing the UDF Plugin<a name="ZH-CN_TOPIC_0000002515622536"></a>

You need to perform the operations described in this section only when your service scenario involves user-defined functions (UDFs). Perform the following plugin operation operations only on the management node. UDFs cannot be accelerated when OmniOperator is enabled for Gluten.

This plugin supports only the HiveSimpleUDF type (simple UDFs written based on the Hive UDF framework). HiveSimpleUDF is built on the Hive UDF framework and is used to extend the set of functions available in Hive queries. Spark supports Hive UDF interfaces. Therefore, HiveSimpleUDF can be directly used in a Spark environment.

OmniOperator accelerates UDFs through row-by-row or batch processing. You can switch between these two processing modes by modifying the configuration file.

**Prerequisites<a name="section15136154016211"></a>**

- Ensure that your UDFs are simple UDFs implemented based on the Hive UDF framework.
- To use OmniOperator to accelerate UDFs, you need to provide related JAR packages and configuration files, including the following files:
- `udf.zip`: contains the class files of all UDFs.
- `conf.zip`: contains the configuration files on which the UDFs depend.
- `udf.properties`: used to configure OmniOperator to accelerate UDFs.

    Using `udfName1` and `udfName2` as examples, the `udf.properties` file has the following format:

    ```shell
    udfName1 com.huawei.udf.UdfName1
    udfName2 com.huawei.udf.UdfName2
    ```

**Installing the UDF Plugin (Row-by-Row Processing)<a name="section5383205172111"></a>**

1. Create the `/opt/omni-operator/hive-udf` directory on the management node.

    ```shell
    mkdir /opt/omni-operator/hive-udf
    ```

2. Upload the `udf.zip` and `conf.zip` packages to the `/opt/omni-operator/hive-udf` directory on the management node.

    >![](public_sys-resources/icon-note.gif) **NOTE:**
    >You can customize the example names of the `udf.zip` and `conf.zip` packages based on your service requirements.

3. Extract the files.

    ```shell
    cd /opt/omni-operator/hive-udf
    unzip udf.zip
    rm -f udf.zip
    unzip conf.zip
    rm -f conf.zip
    ```

4. Modify the `/opt/omni-operator/conf/omni.conf` file.
    1. Open the configuration file.

        ```shell
        vi /opt/omni-operator/conf/omni.conf
        ```

    2. Press `i` to enter the insert mode and add the following UDF configuration.

        ```shell
        # <----UDF properties---->
        # The value "false" indicates expression row-by-row processing and "true" indicates expression batch processing.
        enableBatchExprEvaluate=false
        # UDF trustlist file path.
        hiveUdfPropertyFilePath=./hive-udf/udf.properties
        # Directory of the Hive UDF JAR package.
        hiveUdfDir=./hive-udf/udf
        ```

        >![](public_sys-resources/icon-note.gif) **NOTE:**
        >The directory must start with a period (.). When OmniOperator is running, it reads the value of the `OMNI_HOME` environment variable and uses this value to replace the period (.).

    3. Press `Esc`, type `:wq!`, and press `Enter` to save the file and exit.

5. Update the environment variable.
    1. Open the `~/.bashrc` file.

        ```shell
        vi ~/.bashrc
        ```

    2. Press `i` to enter the insert mode and add `LD_LIBRARY_PATH` to update environment variable.

        ```shell
        export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:${JAVA_HOME}/jre/lib/aarch64/server
        ```

    3. Press `Esc`, type `:wq!`, and press `Enter` to save the file and exit.
    4. Make the updated environment variable take effect.

        ```shell
        source ~/.bashrc
        ```

**Installing the UDF Plugin (Batch Processing)<a name="section518339172217"></a>**

After installing row-by-row processing on the management node, modify the `/opt/omni-operator/conf/omni.conf` file to support batch processing.

1. Open the file.

    ```shell
    vi /opt/omni-operator/conf/omni.conf
    ```

2. Press `i` to enter the insert mode, find the following statement, and modify it.

    ```shell
    enableBatchExprEvaluate=true
    ```

3. Press `Esc`, type `:wq!`, and press `Enter` to save the file and exit.

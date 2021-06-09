# syntax = docker/dockerfile:1.0.2-experimental
# help: a generic toolbox for omnitools. you can (and should) roll your own

# keyserver.ubuntu.com
###############################################################################
## base image - core installs
###############################################################################
FROM debian:buster-slim AS base
LABEL MAINTAINER "Benoit Hudzia <benoit.hudzia@huawei.com>"
SHELL ["/bin/bash", "-eo", "pipefail", "-c"]

ARG HTTP_PROXY=""
RUN DEBIAN_FRONTEND=noninteractive \
    apt-get update -y && \
    apt-get install --no-install-recommends -yq \
      ca-certificates \
      gnupg2=2.2.12-1+deb10u1 && \
    echo deb http://ppa.launchpad.net/apt-fast/stable/ubuntu bionic main >> /etc/apt/sources.list.d/apt-fast.list && \
    echo deb-src http://ppa.launchpad.net/apt-fast/stable/ubuntu bionic main >> /etc/apt/sources.list.d/apt-fast.list && \
    apt-key adv --keyserver-options http-proxy=$HTTP_PROXY --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys A2166B8DE8BDC3367D1901C11EE2FF37CA8DA16B && \
    apt-get update -y && \
    apt-get install --no-install-recommends -yq \
      apt-fast && \
    apt-get clean && \
    apt-fast install --no-install-recommends -yq \
      build-essential \
      python3-dev \
      python3-pip \
      python3-setuptools \
      ca-certificates \
      curl \
      tree \
      git \
      make \
      zip \
      unzip \
      zlib1g-dev \
      telnet \
      vim \
      yamllint \
      wget \
      rpm \
      lsb-release \
      software-properties-common \
      libgtest-dev \
      autoconf \
      cmake \
      shellcheck && \
    apt-fast install --no-install-recommends -yq build-essential clang-tidy && \
    apt-fast install --no-install-recommends -yq linux-perf && \
    apt-fast clean && \
    rm -rf /var/lib/apt/lists/* && \
    dpkg-reconfigure tzdata

## LLVM
RUN echo "check_certificate=off" >> ~/.wgetrc && \
    touch /etc/apt/apt.conf.d/99verify-peer.conf && \
    echo >>/etc/apt/apt.conf.d/99verify-peer.conf "Acquire { https::Verify-Peer false }" && \
    wget  https://apt.llvm.org/llvm.sh && \
    chmod +x llvm.sh && \
    ./llvm.sh 12

## jemalloc
RUN GIT_SSL_NO_VERIFY=true git clone -b 5.2.0 https://github.com/jemalloc/jemalloc.git && \
    cd jemalloc && \
    ./autogen.sh --disable-initial-exec-tls && \
    make && \
    make install


###############################################################################
## java env
###############################################################################
FROM base AS java
RUN curl -kL  https://github.com/AdoptOpenJDK/openjdk8-binaries/releases/download/jdk8u292-b10/OpenJDK8U-jdk_x64_linux_hotspot_8u292b10.tar.gz -o /openjdk8.tgz && \
    mkdir /usr/lib/jvm && \
    tar -zxf openjdk8.tgz -C /usr/lib/jvm && \
    curl -kL https://downloads.apache.org/maven/maven-3/3.8.1/binaries/apache-maven-3.8.1-bin.tar.gz -o /apache-maven-3.8.1-bin.tgz && \
    tar -zxf apache-maven-3.8.1-bin.tgz -C /opt

###############################################################################
## pyenv incase we need it (just a convenience)
###############################################################################
FROM base AS pyenv
RUN GIT_SSL_NO_VERIFY=true git clone https://github.com/pyenv/pyenv.git ~/.pyenv

###############################################################################
## some base python pip installs
###############################################################################
FROM base AS pips
RUN pip3 install --user \
      radon==4.1.0 \
      bandit==1.6.2 \
      pipenv==2020.6.2 \
      flake8==3.8.3 \
      pylint==2.5.3 \
      black==19.10b0 \
      pytest==5.4.3 \
      pytest-html==2.1.1 \
      pytest-xdist==1.32.0 \
      pytest-cov==2.10.0 \
      cfn-lint==0.33.2 \
      sqlfluff==0.3.4

###############################################################################
## hadolint for dockerfile linting
###############################################################################
FROM base AS hadolint
RUN curl -kL https://github.com/hadolint/hadolint/releases/download/v1.18.0/hadolint-Linux-x86_64 -o /usr/bin/hadolint && \
    chmod +x /usr/bin/hadolint

###############################################################################
## terraform linter
###############################################################################
FROM base AS tflint
RUN curl -kL https://github.com/terraform-linters/tflint/releases/download/v0.17.0/tflint_darwin_amd64.zip -o tflint_darwin_amd64.zip && \
    unzip tflint_darwin_amd64.zip && \
    install tflint /usr/local/bin

###############################################################################
## BATS
###############################################################################
FROM base AS bats
RUN GIT_SSL_NO_VERIFY=true git clone https://github.com/bats-core/bats-core.git && \
    ./bats-core/install.sh /usr/local

###############################################################################
## Docker
###############################################################################
FROM base AS docker
SHELL ["/bin/bash", "-o", "pipefail", "-c"]
RUN touch /etc/apt/apt.conf.d/99verify-peer.conf && \
    echo >>/etc/apt/apt.conf.d/99verify-peer.conf "Acquire { https::Verify-Peer false }" && \
    curl -kfsSL https://download.docker.com/linux/debian/gpg | apt-key add - && \
    apt-key fingerprint 0EBFCD88 && \
    echo "deb [arch=amd64] http://download.docker.com/linux/debian buster stable" >> /etc/apt/sources.list.d/docker.list && \
    apt-get update -y && \
    apt-get install -yq --no-install-recommends docker-ce-cli=5:19.03.12~3-0~debian-buster && \
    curl -kL "https://github.com/docker/compose/releases/download/1.26.2/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose && \
    chmod +x /usr/local/bin/docker-compose

###############################################################################
## Golang
###############################################################################
FROM base AS golang
RUN curl -kL https://golang.org/dl/go1.14.4.linux-amd64.tar.gz -o golang.tar.gz && \
    tar -C /usr/local -xzf golang.tar.gz && \
    /usr/local/go/bin/go get -insecure -u golang.org/x/lint/golint && \
    GIT_SSL_NO_VERIFY=true /usr/local/go/bin/go get -insecure -u github.com/securego/gosec/cmd/gosec

###############################################################################
## our published image
###############################################################################
FROM base AS final
COPY  --from=pyenv /root/.pyenv /root/.pyenv
ENV PYENV_ROOT=/root/.pyenv
COPY --from=java /usr/lib/jvm/jdk8u292-b10 /usr/lib/jvm/java-8-openjdk-amd64
COPY --from=java /opt/apache-maven-3.8.1 /opt/apache-maven-3.8.1
RUN ln -s /opt/apache-maven-3.8.1 /opt/maven
ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
ENV M2_HOME=/opt/maven
ENV MAVEN_HOME=/opt/maven
ENV PATH=${M2_HOME}/bin:${PATH}
ENV PATH=$JAVA_HOME/bin:$PATH
ENV PATH=$PATH:/root/.pyenv/bin/:/root/.local/bin:/usr/local/go/bin/:/root/go/bin
#ENV LD_LIBRARY_PATH=/usr/lib:usr/local/lib:/usr/lib/llvm-12/lib
ENV LANG=en-US
RUN printf "if command -v pyenv 1>/dev/null 2>&1; then\n  eval \"\$(pyenv init -)\"\nfi" >> ~/.bash_profile
COPY --from=pips /root/.local/ /root/.local/
COPY --from=hadolint /usr/bin/hadolint /usr/bin/hadolint
COPY --from=tflint /usr/local/bin/tflint /usr/local/bih/tflint
COPY --from=bats /usr/local/bin/bats /usr/local/bin/bats
COPY --from=bats /usr/local/lib/bats-core /usr/local/lib/bats-core
COPY --from=bats /usr/local/libexec/bats-core /usr/local/libexec/bats-core
COPY --from=docker /usr/bin/docker /usr/bin/docker
COPY --from=docker /usr/local/bin/docker-compose /usr/local/bin/docker-compose
COPY --from=golang /usr/local/go /usr/local/go
COPY --from=golang /root/go/bin /root/go/bin
COPY omnitools /etc/omnitools
RUN echo $PATH
RUN ln -sf /etc/omnitools/omnitools.sh /usr/local/bin/omnitools
RUN mkdir $HOME/.m2
RUN ln -sf /etc/omnitools/proxy/settings.xml $HOME/.m2/settings.xml

# Start from the specified base image
FROM --platform=linux/arm64 prestodb/centos7-oj8:latest
MAINTAINER Presto community <https://prestodb.io/community.html>

WORKDIR /opt

# Install required tools 
RUN yum clean all && \
    yum makecache fast && \
    yum install -y epel-release && \
    yum install -y \
        gcc gcc-c++ make automake autoconf libtool \
        pkgconfig \
        openssl-devel \
        snappy snappy-devel \
        java-1.8.0-openjdk-devel \
        zlib zlib-devel \
        libzstd libzstd-devel \
        git \
        wget curl && \
    yum clean all && \
    rm -rf /var/cache/yum

# Set Java environment variables
ENV JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk
ENV PATH=$JAVA_HOME/bin:$PATH

# Set OpenSSL root
ENV OPENSSL_ROOT=/usr/lib64
ENV OPENSSL_ROOT_DIR=$OPENSSL_ROOT
ENV OPENSSL_INCLUDE_DIR=$OPENSSL_ROOT/include
ENV OPENSSL_LIBRARIES=$OPENSSL_ROOT/lib
ENV PKG_CONFIG_PATH=$OPENSSL_ROOT/lib/pkgconfig
ENV LDFLAGS="-L$OPENSSL_ROOT/lib"
ENV CPPFLAGS="-I$OPENSSL_ROOT/include"
ENV CMAKE_PREFIX_PATH=$OPENSSL_ROOT

# Set Zlib environment
ENV ZLIB_HOME=/usr
ENV CMAKE_PREFIX_PATH=$ZLIB_HOME:$CMAKE_PREFIX_PATH
ENV C_INCLUDE_PATH=$ZLIB_HOME/include:$C_INCLUDE_PATH
ENV LIBRARY_PATH=$ZLIB_HOME/lib:$LIBRARY_PATH
# Append Zlib flags to existing LDFLAGS and CPPFLAGS
ENV LDFLAGS="-L$ZLIB_HOME/lib $LDFLAGS"
ENV CPPFLAGS="-I$ZLIB_HOME/include $CPPFLAGS"

# Install Protobuf v3.21.12
# Ensure autogen.sh is executable
RUN curl -L https://github.com/protocolbuffers/protobuf/archive/refs/tags/v3.21.12.tar.gz -o protobuf-3.21.12.tar.gz && \
    tar -zxvf protobuf-3.21.12.tar.gz && \
    cd protobuf-3.21.12 && \
    chmod +x autogen.sh && \
    ./autogen.sh && \
    ./configure --prefix=/usr/local && \
    make -j$(nproc) && \
    make install && \
    ldconfig && \
    cd .. && rm -rf protobuf-3.21.12 protobuf-3.21.12.tar.gz

# Install CMake 3.22.3 from source
RUN cd /tmp && \
    wget https://github.com/Kitware/CMake/releases/download/v3.22.3/cmake-3.22.3.tar.gz && \
    tar -zxvf cmake-3.22.3.tar.gz && \
    cd cmake-3.22.3 && \
    ./bootstrap && \
    make -j$(nproc) && \
    make install && \
    cd / && rm -rf /tmp/cmake-3.22.3*

# Install Apache Maven 3.9.6
ENV MAVEN_VERSION=3.9.6
ENV MAVEN_HOME=/opt/apache-maven-${MAVEN_VERSION}
ENV PATH=$MAVEN_HOME/bin:$PATH

RUN curl -fSL https://archive.apache.org/dist/maven/maven-3/${MAVEN_VERSION}/binaries/apache-maven-${MAVEN_VERSION}-bin.tar.gz -o /tmp/apache-maven.tar.gz && \
    tar -xzf /tmp/apache-maven.tar.gz -C /opt && \
    rm /tmp/apache-maven.tar.gz

# Clone Hadoop and checkout
RUN git clone https://github.com/apache/hadoop.git /opt/hadoop && \
    cd /opt/hadoop && \
    git checkout branch-3.4.1

WORKDIR /opt/hadoop

# Build Hadoop Common with native libs, including Zstandard
RUN mvn clean package -pl hadoop-common-project/hadoop-common -am \
    -Pdist,native \
    -DskipTests \
    -Dtar \
    -Drequire.snappy \
    -Drequire.zstd \
    -Dmaven.javadoc.skip=true
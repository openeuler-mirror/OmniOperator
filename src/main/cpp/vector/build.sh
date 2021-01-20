compile() {
    export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/;
    javac -h include ../../java/nova/hetu/omnicache/*.java

    #cargo build
    g++ -fPIC -I"$JAVA_HOME/include" -I"$JAVA_HOME/include/linux" -shared -o target/libomnicache.so OMVector.cpp
}

compile

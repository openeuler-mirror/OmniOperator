compile() {
    export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/;
    javac -h include ../../java/nova/hetu/omnicache/*.java

    gcc -fPIC -I"$JAVA_HOME/include" -I"$JAVA_HOME/include/linux" -shared -o omnicache.so OMVector.cpp
}

compile

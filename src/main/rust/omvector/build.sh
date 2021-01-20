compile() {
    export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/;
    javac -h include ../../java/nova/hetu/omnicache/*.java

    cargo build
}

compile

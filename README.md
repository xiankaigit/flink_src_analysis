# flink_src_analysis
1. 编译Flink
    1.1 手动下载kafka-schema-registry-client-5.5.2.jar（https://packages.confluent.io/maven/io/confluent/kafka-schema-registry-client/5.5.2/）
    1.2 安装这个jar包，mvn install:install-file -DgroupId=io.confluent -DartifactId=kafka-schema-registry-client -Dversion=5.5.2 -Dpackaging=jar  -Dfile=/home/xk/Downloads/kafka-schema-registry-client-5.5.2.jar
    1.3 编译（jdk11）：mvn clean install -DskipTests=true -Drat.skip=true -Dcheckstyle.skip=true -Dfast
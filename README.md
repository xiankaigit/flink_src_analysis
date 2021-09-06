# flink_src_analysis
# 1. 编译Flink
    1.1 手动下载kafka-schema-registry-client-5.5.2.jar（https://packages.confluent.io/maven/io/confluent/kafka-schema-registry-client/5.5.2/）  
    1.2 安装这个jar包，mvn install:install-file -DgroupId=io.confluent -DartifactId=kafka-schema-registry-client -Dversion=5.5.2 -Dpackaging=jar  -Dfile=/home/xk/Downloads/kafka-schema-registry-client-5.5.2.jar  
    1.3 编译（jdk11）：mvn clean install -DskipTests=true -Drat.skip=true -Dcheckstyle.skip=true -Dfast
# 2. 集群启动
## 2.1 JM启动流程（以Standalone模式为例子）
基本流程如下：  
step1: 加载插件：加载flink安装路径下面的plugins目录下的jar包，创建插件（例如与metric相关的），创建插件管理器   
step2: 初始化JM中的相关服务  
  * metricRegistry: 跟踪所有已经注册的Metric
  * haServices: 高可用相关的服务，主要实现分布式计算和leader选举
  * blobServer: 服务创建与任务相关的存储目录结构，用于存储用户上传的jar包等
  * heartbeatServices:  用户心跳检测的服务
  * commonRpcService: 基于akka的rpc通信服务，用于集群间通信（JM/TM等之间的通信，不涉及JOB数据交换，Job上下游任务task的数据交换是tm中通过netty实现的）
  * archivedExecutionGraphStore: 存储序列化后的EexcutionGraph   
  
step3: 构造clusterComponent,包含三大组件  
  * Dispatcher: 负责用于接收作业提交，持久化它们，生成要执行的作业管理器任务，并在主任务失败时恢复它们。此外,它知道关于Flink会话集群的状态。
  * ResourceManager: 负责资源调度
  * WebMonitorEndpoint:服务于web前端Rest调用的Rest端点
# flink_src_analysis
# 1. 编译Flink
    1.1 手动下载kafka-schema-registry-client-5.5.2.jar（https://packages.confluent.io/maven/io/confluent/kafka-schema-registry-client/5.5.2/）  
    1.2 安装这个jar包，mvn install:install-file -DgroupId=io.confluent -DartifactId=kafka-schema-registry-client -Dversion=5.5.2 -Dpackaging=jar  -Dfile=/home/xk/Downloads/kafka-schema-registry-client-5.5.2.jar  
    1.3 编译（jdk11）：mvn clean install -DskipTests=true -Drat.skip=true -Dcheckstyle.skip=true -Dfast
# 2.源码阅读方式
本人直接在idea里跑flink，没有采用远程debug的方式，具体方式如下：
2.1 配置系统变量（/etc/profile）
    本地启动需要加载一些flink的相关配置和插件，这个时候最好讲flink的安装包解压(如/home/hik/apps/flink-1.12.0)，在profile中配置相关变量（无需启动flink）  

    export FLINK_CONF_DIR=/home/hik/apps/flink-1.12.0/conf
    export FLINK_BIN_DIR=/home/hik/apps/flink-1.12.0/bin
    export FLINK_PLUGINS_DIR=/home/hik/apps/flink-1.12.0/plugins
    export FLINK_LIB_DIR=/home/hik/apps/flink-1.12.0/lib
    export FLINK_OPT_DIR=/home/hik/apps/flink-1.12.0/opt  
 备注：flink正常启动的话不需要配置这些变量，因为启动的过程会执行脚本config.sh这些变量会自动export  
2.2 启动集群  
    集群的启动相关入口，我写在了flink-runtime-web（这样启动的时候能够有flink-ui）的单元测试中,如
    standalone启动模式：  
    * JM启动：Lorg.apache.flink.runtime.entrypoint.standalone.JmLocalRun  
    * TM启动：org.apache.flink.runtime.entrypoint.standalone.TmLocalRun  
    yarn模式：  
    * PerJob模式： org.apache.flink.runtime.entrypoint.yarn.perjob.PerJobCliTest  
    * yarn-session: org.apache.flink.runtime.entrypoint.yarn.session.FlinkYarnSessionCliTest
# 3. 集群启动
以Standalone模式为例
## 3.1 JM启动流程
注意：  
* 这里启动的其实是一个master服务，这个master只是包含一些用于支撑master运行的服务（下面有说），严格意义（代码层面），这里这个master并不是指的JobMaster或者JobManager,JobMaster或者JobManager是在提交任务的时候才会初始化的，后面，具体任务执行流程一部分。  
* 这里的Jm指的是JobMaster,老的代码中JM是指的JobManager, 最新的代码中其实已经是用JobMaster代理了JobManager的作用,原先的JobManager(JobManagerImpl)其实是负责单个Job的管理，相当于JobMaster中的一个服务或对象，管理一个job,不同的job有不同的额JobManager
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
## 3.2 TM启动流程（以Standalone模式为例子）
step1: 加载插件： 和jm启动类似，加载flink安装路径下面的plugins目录下的jar包  
step2: 初始化共享文件系统配置(FileSystemFactory)，比如HDFS或本地文件系统  
step3: 创建TM(TaskManagerRunner)  
    注意：TaskManagerRunner最底层是taskManagerServices，taskManagerServices经过包装形成taskExecutor，然后taskManagerServices经过包装形成TaskManagerRunner，如下：  
    TaskManagerRunner-->taskExecutor-->taskManagerServices
    由于最底层是taskManagerServices，所以我在里只写出taskManagerServices的一个基本初始化流程：    

    step3.1  创建TaskEventDispatcher  
    step3.2  创建异步IO管理器  
    step3.3 创建ShuffleEnvironment,这个非常重要,后面分析数据交换再分析  
    step3.4 创建kv状态存储服务  
    step3.5 创建广播变量管理器  
    step3.6 创建任务状态管理器  
step4: 启动tm,这里其实就是taskManagerRunner的start中逐层的往里调用最终到了TaskExecutor的startTaskExecutorServices:  

    step4.1 启动leader变更监听服务  
    step4.2 启动taskslottable  
    step4.3 启动job leader服务
# 4 任务提交流程 
## 4.1 基本概念与流程
Flink 中的执行图可以分成四层
* StreamGraph：根据用户通过 Stream API 编写的代码生成的最初的图.用来表示程序拓扑结构。  
* JobGraph：StreamGraph经过优化后生成了 JobGraph，提交给 JobManager 的数据结构。.主要的优化为，将多个符合条件的节点 chain 在一起作为一个节点，这样可以减少数据在节点之间流动所需要的序列化/反序列化/传输消耗。
* ExecutionGraph：JobManager 根据 JobGraph 生成的分布式执行图，是调度层最核心的数据结构.
* 物理执行图：JobManager 根据 ExecutionGraph 对 Job 进行调度后，在各个TaskManager 上部署 Task 后形成的“图”，并不是一个具体的数据结构.
四张图
## 4.2 构造StreamGraph
注意： 这一过程是在client端进行的
### 4.2.1 构造构造TransformationTree
我们在编写Flink任务事，会调用DataStream中的各种方法，如map,flatmap,filter，调用这些方法时，会传入对应自定义的Function(如MapFuntion，FlatMapFunction)，在map,flatmap,filter的内部会对Function
进行一系列的包装(代码中我以flatmap为例进行了分析并添加了注释):  
1）将Function包装成StreamOperator,例如讲FlatMapFunction包装成StreamFlatMap  
2)讲StreamOperator包装成Transformation，例如讲转换成OneInputTransformation
3)这个Transformation会加入到env的一个成员变量中（list类型）构成TransformationTree 

    吐槽：Flink源码中有些类和方法的命名，真的不符合见名知意要求，我这里以flatmap里面上面一系列的包装说明，源码中我也加了注释,我们在DataStram上调用flatmap后会进行以下转换（符合上面我们分析的流程）
    step1: FlatMapFunction(用户传入)的最顶层实现为Function, flatmap内部会将其包装成StreamFlatMap
    step2: StreamFlatMap实现自StreamOperator,这个StreamOperator会进一步封装为OneInputTransformation
    step3. Transformation会加入到env的一个成员变量中（list）构成TransformationTree
    OneInputTransformation实现自Transformation. 到这里命名都没有问题， 但是我们调用玩flatmap是不是要返回一个新的DataStream给用户呢，这个新的DataStream是如何形成的呢?
    答案是通过最终的Transformation和env初始化需要返回的这个DataStram,这个DataStream就是我们调用算子的返回值。
    具体来说在flatmap中是通过OneInputTransformation和env构造的新的DataStream,具体点就是继承了DataStream的SingleOutputStreamOperator，将这个SingleOutputStreamOperator实例返回给用户。  
    对没错，SingleOutputStreamOperator是DataStream的子类，而不是StreamOperator的实现类,所以我就想说SingleOutputStreamOperator既然是是DataStream的子类，为什么不叫SingleOutputStream。
    此外，我们在构造完Transformation，会讲Transformation加入到env的成员变量中（list）,会调用env的一个方法，这个方法的名字叫做addOperator,名字虽然叫addOperator，但参数却是tranformation类型的   
    Flink的开发人员估计有别的考虑，所以才这么命名的，目前我没有看懂为什么这么命名，对于我这样一个对代码洁癖的人来说，看的真心难受，个人的一点点想法，可能想的不周勿喷～

### 4.2.2 构造SteamGraph
执行env.execute()的时候，env内部会将上一步构造的TransformationTree作为参数调用StreamGraphGenerator的generate方法构造StreamGraph  
StreamGraphGenerator的generate方法便利TransformationTree，对每一个Transformation转换为StreamNode和Edge加到StreamGraph上，转换逻辑如下：  
step1: 将当前的Transformation构造成StreamNode添加到StreamGraph上  
step2: 将当前的Transformation和它的每一个父节点构造成Edge添加到StreamGraph上.  
注意：Edge是当前Transformation和父Transformation之间的链接，所以在处理当前节点的时候，其全部父Transformation都必须转换完毕，因此在对当前的Transformation进行转换
之前会判断每一父Transformation是否已经转换，如果没有先转换父节点，相当于递归了。  

    备注：上面吐槽了一下，现在可以点赞一波了：对比老的StreamGraph生成逻辑中，我发现还有一点代码结构上的优化，例如针对不同的Transformation进行转化生成StreamNode和Edge的时候，  
    新老版本的Flink都根据Transformation的类型不同有不同的处理，但是新版本的显然更优雅：
    老的版本直接就是 通过if else if else if .....else,10多个分之的判断，在当年看老版本代码的时候，我就吐槽过并且有自己优化的想法，结果新的版本和我想法是一致的
    新的版本是通过多态来解决的（这个也是我们解决if-else多分支问题的常用方法，高性能java书中有说过），讲转换逻辑封根据Transformation封装成不同的TransformationTranslator,并将能够处理的Transformation类型（Class）作为key，TransformationTranslator,作为value放到map中，  
    转换具体的Transformation时，讲要转换的Transformation作为key去这个map中去寻找能够转换的TransformationTranslator,然后处理，很优雅的解决了if-else丑陋问题。
## 4.3 构造JobGraph

step1 构造散列值:并为每个SteamNode生成散列值，应用的算法那可以保证如果提交的拓扑没有改变，则每次生成的散列值都是一样的，一个StreamNode的ID对应一个散列值(广度优先算法)  
step2 构造hash值  
step3 设置chain(重要,具体逻辑看这一步内部的具体代码注释)  
step4 将每个JobVertex的入边集合也序列化到该JobVertex的StreamConfig中（出边集合已经在上一步的时候写入了）  
step5 据group name，为每个 JobVertex 指定所属的 SlotSharingGroup,以及针对 Iteration的头尾设置  CoLocationGroup  
step6 配置内存相关的参数(权重比)  
step7 配置checkpoint  
step8 设置SavePoint相关参数  
step9 添加用户分布式缓存  
step10 设置 job execution 配置  

## 4.4 构造ExecutionGraph
step1:准备工作：进行用于类加载器的获取，重启策略的设置；  
step2:通过JobVertex构造ExeJobVertex;  
step3:设置监听器，监听后面的运行；  
step4:分配slot信息并提交执行

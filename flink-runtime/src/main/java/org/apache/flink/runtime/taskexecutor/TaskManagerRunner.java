/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.taskexecutor;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JMXServerOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.configuration.TaskManagerOptionsInternal;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.plugin.PluginManager;
import org.apache.flink.core.plugin.PluginUtils;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.blob.BlobCacheService;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.entrypoint.FlinkParseException;
import org.apache.flink.runtime.externalresource.ExternalResourceInfoProvider;
import org.apache.flink.runtime.externalresource.ExternalResourceUtils;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesUtils;
import org.apache.flink.runtime.io.network.partition.TaskExecutorPartitionTrackerImpl;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalException;
import org.apache.flink.runtime.management.JMXService;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.MetricRegistryConfiguration;
import org.apache.flink.runtime.metrics.MetricRegistryImpl;
import org.apache.flink.runtime.metrics.ReporterSetup;
import org.apache.flink.runtime.metrics.groups.TaskManagerMetricGroup;
import org.apache.flink.runtime.metrics.util.MetricUtils;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.akka.AkkaRpcServiceUtils;
import org.apache.flink.runtime.security.SecurityConfiguration;
import org.apache.flink.runtime.security.SecurityUtils;
import org.apache.flink.runtime.taskmanager.MemoryLogger;
import org.apache.flink.runtime.util.ConfigurationParserUtils;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.apache.flink.runtime.util.Hardware;
import org.apache.flink.runtime.util.JvmShutdownSafeguard;
import org.apache.flink.runtime.util.LeaderRetrievalUtils;
import org.apache.flink.runtime.util.SignalHandler;
import org.apache.flink.util.AbstractID;
import org.apache.flink.util.AutoCloseableAsync;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.ExecutorUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.StringUtils;
import org.apache.flink.util.TaskManagerExceptionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.UndeclaredThrowableException;
import java.net.InetAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.runtime.security.ExitTrappingSecurityManager.replaceGracefulExitWithHaltIfConfigured;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This class is the executable entry point for the task manager in yarn or standalone mode.
 * It constructs the related components (network, I/O manager, memory manager, RPC service, HA service)
 * and starts them.
 */
public class TaskManagerRunner implements FatalErrorHandler, AutoCloseableAsync {

	private static final Logger LOG = LoggerFactory.getLogger(TaskManagerRunner.class);

	private static final long FATAL_ERROR_SHUTDOWN_TIMEOUT_MS = 10000L;

	private static final int STARTUP_FAILURE_RETURN_CODE = 1;

	@VisibleForTesting
	static final int RUNTIME_FAILURE_RETURN_CODE = 2;

	private final Object lock = new Object();

	private final Configuration configuration;

	private final ResourceID resourceId;

	private final Time timeout;

	private final RpcService rpcService;

	private final HighAvailabilityServices highAvailabilityServices;

	private final MetricRegistryImpl metricRegistry;

	private final BlobCacheService blobCacheService;

	/** Executor used to run future callbacks. */
	private final ExecutorService executor;

	private final TaskExecutorService taskExecutorService;

	private final CompletableFuture<Void> terminationFuture;

	private boolean shutdown;

	public TaskManagerRunner(
			Configuration configuration,
			PluginManager pluginManager,
			TaskExecutorServiceFactory taskExecutorServiceFactory) throws Exception {
		this.configuration = checkNotNull(configuration);

		timeout = AkkaUtils.getTimeoutAsTime(configuration);

		this.executor = java.util.concurrent.Executors.newScheduledThreadPool(
			Hardware.getNumberCPUCores(),
			new ExecutorThreadFactory("taskmanager-future"));

		//创建HA服务,standlone模式对应的是StandaloneHaServices，其实这个是假的高可用,不会实质的参与JM高可用
		highAvailabilityServices = HighAvailabilityServicesUtils.createHighAvailabilityServices(
			configuration,
			executor,
			HighAvailabilityServicesUtils.AddressResolution.NO_ADDRESS_RESOLUTION);

		//启动一个JmxServer, 用于metric
		JMXService.startInstance(configuration.getString(JMXServerOptions.JMX_SERVER_PORT));
	 	//给予akka实现的Rpc服务，用于Flink进程间通信，注意不用于task的上下游的数据交换
		rpcService = createRpcService(configuration, highAvailabilityServices);

		this.resourceId = getTaskManagerResourceID(configuration, rpcService.getAddress(), rpcService.getPort());
		//用于心跳检测的服务
		HeartbeatServices heartbeatServices = HeartbeatServices.fromConfiguration(configuration);

		// 创建监控指标注册服务
		// 用于记录所有的metrics，连接MetricGroup和MetricReporter
		metricRegistry = new MetricRegistryImpl(
			MetricRegistryConfiguration.fromConfiguration(configuration),
			ReporterSetup.fromConfiguration(configuration, pluginManager));
         //开启metrics查询服务
		final RpcService metricQueryServiceRpcService = MetricUtils.startRemoteMetricsRpcService(configuration, rpcService.getAddress());
		metricRegistry.startQueryService(metricQueryServiceRpcService, resourceId);

		blobCacheService = new BlobCacheService(
			configuration, highAvailabilityServices.createBlobStore(), null
		);

		final ExternalResourceInfoProvider externalResourceInfoProvider =
			ExternalResourceUtils.createStaticExternalResourceInfoProvider(
				ExternalResourceUtils.getExternalResourceAmountMap(configuration),
				ExternalResourceUtils.externalResourceDriversFromConfig(configuration, pluginManager));

		taskExecutorService = taskExecutorServiceFactory.createTaskExecutor(
			this.configuration,
			this.resourceId,
			rpcService,
			highAvailabilityServices,
			heartbeatServices,
			metricRegistry,
			blobCacheService,
			false,
			externalResourceInfoProvider,
			this);

		this.terminationFuture = new CompletableFuture<>();
		this.shutdown = false;
		handleUnexpectedTaskExecutorServiceTermination();

		MemoryLogger.startIfConfigured(LOG, configuration, terminationFuture);
	}

	private void handleUnexpectedTaskExecutorServiceTermination() {
		taskExecutorService.getTerminationFuture().whenComplete((unused, throwable) -> {
			synchronized (lock) {
				if (!shutdown) {
					onFatalError(new FlinkException("Unexpected termination of the TaskExecutor.", throwable));
				}
			}
		});
	}

	// --------------------------------------------------------------------------------------------
	//  Lifecycle management
	// --------------------------------------------------------------------------------------------

	public void start() throws Exception {
		taskExecutorService.start();
	}

	@Override
	public CompletableFuture<Void> closeAsync() {
		synchronized (lock) {
			if (!shutdown) {
				shutdown = true;

				final CompletableFuture<Void> taskManagerTerminationFuture = taskExecutorService.closeAsync();

				final CompletableFuture<Void> serviceTerminationFuture = FutureUtils.composeAfterwards(
					taskManagerTerminationFuture,
					this::shutDownServices);

				serviceTerminationFuture.whenComplete(
					(Void ignored, Throwable throwable) -> {
						if (throwable != null) {
							terminationFuture.completeExceptionally(throwable);
						} else {
							terminationFuture.complete(null);
						}
					});
			}
		}

		return terminationFuture;
	}

	private CompletableFuture<Void> shutDownServices() {
		synchronized (lock) {
			Collection<CompletableFuture<Void>> terminationFutures = new ArrayList<>(3);
			Exception exception = null;

			try {
				JMXService.stopInstance();
			} catch (Exception e) {
				exception = ExceptionUtils.firstOrSuppressed(e, exception);
			}

			try {
				blobCacheService.close();
			} catch (Exception e) {
				exception = ExceptionUtils.firstOrSuppressed(e, exception);
			}

			try {
				metricRegistry.shutdown();
			} catch (Exception e) {
				exception = ExceptionUtils.firstOrSuppressed(e, exception);
			}

			try {
				highAvailabilityServices.close();
			} catch (Exception e) {
				exception = ExceptionUtils.firstOrSuppressed(e, exception);
			}

			terminationFutures.add(rpcService.stopService());

			terminationFutures.add(ExecutorUtils.nonBlockingShutdown(timeout.toMilliseconds(), TimeUnit.MILLISECONDS, executor));

			if (exception != null) {
				terminationFutures.add(FutureUtils.completedExceptionally(exception));
			}

			return FutureUtils.completeAll(terminationFutures);
		}
	}

	// export the termination future for caller to know it is terminated
	public CompletableFuture<Void> getTerminationFuture() {
		return terminationFuture;
	}

	// --------------------------------------------------------------------------------------------
	//  FatalErrorHandler methods
	// --------------------------------------------------------------------------------------------

	@Override
	public void onFatalError(Throwable exception) {
		TaskManagerExceptionUtils.tryEnrichTaskManagerError(exception);
		LOG.error("Fatal error occurred while executing the TaskManager. Shutting it down...", exception);

		// In case of the Metaspace OutOfMemoryError, we expect that the graceful shutdown is possible,
		// as it does not usually require more class loading to fail again with the Metaspace OutOfMemoryError.
		if (ExceptionUtils.isJvmFatalOrOutOfMemoryError(exception) &&
				!ExceptionUtils.isMetaspaceOutOfMemoryError(exception)) {
			terminateJVM();
		} else {
			closeAsync();

			FutureUtils.orTimeout(terminationFuture, FATAL_ERROR_SHUTDOWN_TIMEOUT_MS, TimeUnit.MILLISECONDS);

			terminationFuture.whenComplete(
				(Void ignored, Throwable throwable) -> terminateJVM());
		}
	}

	private void terminateJVM() {
		System.exit(RUNTIME_FAILURE_RETURN_CODE);
	}

	// --------------------------------------------------------------------------------------------
	//  Static entry point
	// --------------------------------------------------------------------------------------------

	public static void main(String[] args) throws Exception {
		String arg_param= org.apache.commons.lang3.StringUtils.join(args, ",");
		String className = Thread.currentThread().getStackTrace()[1].getClassName();
		int idx = className.lastIndexOf(".")+1;
		String simpleName =className.substring(idx);
		System.out.println(simpleName + " commond line is "+arg_param);
		LOG.info(simpleName+" commond line is "+arg_param);
		// startup checks and logging
		EnvironmentInformation.logEnvironmentInfo(LOG, "TaskManager", args);
		SignalHandler.register(LOG);
		//设置JVM关闭前的回调
		JvmShutdownSafeguard.installAsShutdownHook(LOG);

		long maxOpenFileHandles = EnvironmentInformation.getOpenFileHandlesLimit();

		if (maxOpenFileHandles != -1L) {
			LOG.info("Maximum number of open file descriptors is {}.", maxOpenFileHandles);
		} else {
			LOG.info("Cannot determine the maximum number of open file descriptors");
		}

		runTaskManagerSecurely(args);
	}

	public static Configuration loadConfiguration(String[] args) throws FlinkParseException {
		return ConfigurationParserUtils.loadCommonConfiguration(args, TaskManagerRunner.class.getSimpleName());
	}

	public static void runTaskManager(Configuration configuration, PluginManager pluginManager) throws Exception {
		//准备工作结束，正式启动tm
		//step1: 创建一个TaskManagerRunner
		final TaskManagerRunner taskManagerRunner = new TaskManagerRunner(configuration, pluginManager, TaskManagerRunner::createTaskExecutorService);
        //step2: 启动tm,这里其实就是taskManagerRunner的start中逐层的往里调用最终到了；额，其实就是启动相关RPC的服务网
		taskManagerRunner.start();
	}

	public static void runTaskManagerSecurely(String[] args) {
		try {
			//解析flink配置文件和命令行传入的参数，构造config
			Configuration configuration = loadConfiguration(args);
			runTaskManagerSecurely(configuration);
		} catch (Throwable t) {
			final Throwable strippedThrowable = ExceptionUtils.stripException(t, UndeclaredThrowableException.class);
			LOG.error("TaskManager initialization failed.", strippedThrowable);
			System.exit(STARTUP_FAILURE_RETURN_CODE);
		}
	}

	public static void runTaskManagerSecurely(Configuration configuration) throws Exception {
		replaceGracefulExitWithHaltIfConfigured(configuration);
		//加载flink安装路径下面的plugins目录下的jar包，创建插件管理器,这些插件是单独的类加载器加载的
		final PluginManager pluginManager = PluginUtils.createPluginManagerFromRootFolder(configuration);
		// 初始化共享文件系统配置(FileSystemFactory)，比如HDFS
		FileSystem.initialize(configuration, pluginManager);

		SecurityUtils.install(new SecurityConfiguration(configuration));

		SecurityUtils.getInstalledContext().runSecured(() -> {
			runTaskManager(configuration, pluginManager);
			return null;
		});
	}

	// --------------------------------------------------------------------------------------------
	//  Static utilities
	// --------------------------------------------------------------------------------------------

	public static TaskExecutorService createTaskExecutorService(
			Configuration configuration,
			ResourceID resourceID,
			RpcService rpcService,
			HighAvailabilityServices highAvailabilityServices,
			HeartbeatServices heartbeatServices,
			MetricRegistry metricRegistry,
			BlobCacheService blobCacheService,
			boolean localCommunicationOnly,
			ExternalResourceInfoProvider externalResourceInfoProvider,
			FatalErrorHandler fatalErrorHandler) throws Exception {

		final TaskExecutor taskExecutor = startTaskManager(
				configuration,
				resourceID,
				rpcService,
				highAvailabilityServices,
				heartbeatServices,
				metricRegistry,
				blobCacheService,
				localCommunicationOnly,
				externalResourceInfoProvider,
				fatalErrorHandler);

		return TaskExecutorToServiceAdapter.createFor(taskExecutor);
	}

	public static TaskExecutor startTaskManager(
			Configuration configuration,
			ResourceID resourceID,
			RpcService rpcService,
			HighAvailabilityServices highAvailabilityServices,
			HeartbeatServices heartbeatServices,
			MetricRegistry metricRegistry,
			BlobCacheService blobCacheService,
			boolean localCommunicationOnly,
			ExternalResourceInfoProvider externalResourceInfoProvider,
			FatalErrorHandler fatalErrorHandler) throws Exception {

		checkNotNull(configuration);
		checkNotNull(resourceID);
		checkNotNull(rpcService);
		checkNotNull(highAvailabilityServices);

		LOG.info("Starting TaskManager with ResourceID: {}", resourceID.getStringWithMetadata());

		String externalAddress = rpcService.getAddress();

		//tm资源信息，例如cpu核数，堆内存，堆外内存，用于网络通信的内存，managedmemory
		final TaskExecutorResourceSpec taskExecutorResourceSpec = TaskExecutorResourceUtils.resourceSpecFromConfig(configuration);

		TaskManagerServicesConfiguration taskManagerServicesConfiguration =
			TaskManagerServicesConfiguration.fromConfiguration(
				configuration,
				resourceID,
				externalAddress,
				localCommunicationOnly,
				taskExecutorResourceSpec);
		// 创建task manager的MetricGroup
		Tuple2<TaskManagerMetricGroup, MetricGroup> taskManagerMetricGroup = MetricUtils.instantiateTaskManagerMetricGroup(
			metricRegistry,
			externalAddress,
			resourceID,
			taskManagerServicesConfiguration.getSystemResourceMetricsProbingInterval());

		// 创建用于IO任务的线程池
		final ExecutorService ioExecutor = Executors.newFixedThreadPool(
			taskManagerServicesConfiguration.getNumIoThreads(),
			new ExecutorThreadFactory("flink-taskexecutor-io"));

		// 创建Task Manager服务，具体看里面的注释，非常重要
		TaskManagerServices taskManagerServices = TaskManagerServices.fromConfiguration(
			taskManagerServicesConfiguration,
			blobCacheService.getPermanentBlobService(),
			taskManagerMetricGroup.f1,
			ioExecutor,
			fatalErrorHandler);

		MetricUtils.instantiateFlinkMemoryMetricGroup(
			taskManagerMetricGroup.f1,
			taskManagerServices.getTaskSlotTable(),
			taskManagerServices::getManagedMemorySize);

		TaskManagerConfiguration taskManagerConfiguration =
			TaskManagerConfiguration.fromConfiguration(configuration, taskExecutorResourceSpec, externalAddress);

		String metricQueryServiceAddress = metricRegistry.getMetricQueryServiceGatewayRpcAddress();

		return new TaskExecutor(
			rpcService,
			taskManagerConfiguration,
			highAvailabilityServices,
			taskManagerServices,
			externalResourceInfoProvider,
			heartbeatServices,
			taskManagerMetricGroup.f0,
			metricQueryServiceAddress,
			blobCacheService,
			fatalErrorHandler,
			new TaskExecutorPartitionTrackerImpl(taskManagerServices.getShuffleEnvironment()),
			createBackPressureSampleService(configuration, rpcService.getScheduledExecutor()));
	}

	static BackPressureSampleService createBackPressureSampleService(
			Configuration configuration,
			ScheduledExecutor scheduledExecutor) {
		return new BackPressureSampleService(
			configuration.getInteger(WebOptions.BACKPRESSURE_NUM_SAMPLES),
			Time.milliseconds(configuration.getInteger(WebOptions.BACKPRESSURE_DELAY)),
			scheduledExecutor);
	}

	/**
	 * Create a RPC service for the task manager.
	 *
	 * @param configuration The configuration for the TaskManager.
	 * @param haServices to use for the task manager hostname retrieval
	 */
	@VisibleForTesting
	static RpcService createRpcService(
		final Configuration configuration,
		final HighAvailabilityServices haServices) throws Exception {

		checkNotNull(configuration);
		checkNotNull(haServices);

		return AkkaRpcServiceUtils.createRemoteRpcService(
			configuration,
			determineTaskManagerBindAddress(configuration, haServices),
			configuration.getString(TaskManagerOptions.RPC_PORT),
			configuration.getString(TaskManagerOptions.BIND_HOST),
			configuration.getOptional(TaskManagerOptions.RPC_BIND_PORT));
	}

	private static String determineTaskManagerBindAddress(
			final Configuration configuration,
			final HighAvailabilityServices haServices) throws Exception {

		final String configuredTaskManagerHostname = configuration.getString(TaskManagerOptions.HOST);

		if (configuredTaskManagerHostname != null) {
			LOG.info("Using configured hostname/address for TaskManager: {}.", configuredTaskManagerHostname);
			return configuredTaskManagerHostname;
		} else {
			return determineTaskManagerBindAddressByConnectingToResourceManager(configuration, haServices);
		}
	}

	private static String determineTaskManagerBindAddressByConnectingToResourceManager(
			final Configuration configuration,
			final HighAvailabilityServices haServices) throws LeaderRetrievalException {

		final Duration lookupTimeout = AkkaUtils.getLookupTimeout(configuration);

		final InetAddress taskManagerAddress = LeaderRetrievalUtils.findConnectingAddress(
			haServices.getResourceManagerLeaderRetriever(),
			lookupTimeout);

		LOG.info("TaskManager will use hostname/address '{}' ({}) for communication.",
			taskManagerAddress.getHostName(), taskManagerAddress.getHostAddress());

		HostBindPolicy bindPolicy = HostBindPolicy.fromString(configuration.getString(TaskManagerOptions.HOST_BIND_POLICY));
		return bindPolicy == HostBindPolicy.IP ? taskManagerAddress.getHostAddress() : taskManagerAddress.getHostName();
	}

	@VisibleForTesting
	static ResourceID getTaskManagerResourceID(Configuration config, String rpcAddress, int rpcPort) throws Exception {
		return new ResourceID(
			config.getString(TaskManagerOptions.TASK_MANAGER_RESOURCE_ID,
				StringUtils.isNullOrWhitespaceOnly(rpcAddress)
					? InetAddress.getLocalHost().getHostName() + "-" + new AbstractID().toString().substring(0, 6)
					: rpcAddress + ":" + rpcPort + "-" + new AbstractID().toString().substring(0, 6)),
			config.getString(TaskManagerOptionsInternal.TASK_MANAGER_RESOURCE_ID_METADATA, ""));
	}

	/**
	 * Factory for {@link TaskExecutor}.
	 */
	public interface TaskExecutorServiceFactory {
		TaskExecutorService createTaskExecutor(
				Configuration configuration,
				ResourceID resourceID,
				RpcService rpcService,
				HighAvailabilityServices highAvailabilityServices,
				HeartbeatServices heartbeatServices,
				MetricRegistry metricRegistry,
				BlobCacheService blobCacheService,
				boolean localCommunicationOnly,
				ExternalResourceInfoProvider externalResourceInfoProvider,
				FatalErrorHandler fatalErrorHandler) throws Exception;
	}

	public interface TaskExecutorService extends AutoCloseableAsync {
		void start();

		CompletableFuture<Void> getTerminationFuture();
	}
}

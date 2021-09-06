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

package org.apache.flink.runtime.entrypoint;

import org.apache.commons.lang3.StringUtils;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.entrypoint.component.DefaultDispatcherResourceManagerComponentFactory;
import org.apache.flink.runtime.resourcemanager.StandaloneResourceManagerFactory;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.runtime.util.JvmShutdownSafeguard;
import org.apache.flink.runtime.util.SignalHandler;

import java.util.Map;

/**
 * Entry point for the standalone session cluster.
 */
public class StandaloneSessionClusterEntrypoint extends SessionClusterEntrypoint {

	public StandaloneSessionClusterEntrypoint(Configuration configuration) {
		super(configuration);
	}

	@Override
	protected DefaultDispatcherResourceManagerComponentFactory createDispatcherResourceManagerComponentFactory(Configuration configuration) {
		return DefaultDispatcherResourceManagerComponentFactory.createSessionComponentFactory(StandaloneResourceManagerFactory.getInstance());
	}

	public static void main(String[] args) {
		String arg_param= StringUtils.join(args, ",");
		String className = Thread.currentThread().getStackTrace()[1].getClassName();
		int idx = className.lastIndexOf(".")+1;
		String simpleName =className.substring(idx);
		System.out.println(simpleName + " commond line is "+arg_param);
		LOG.info(simpleName+" commond line is "+arg_param);
		Map<String, String> env = System.getenv();
		LOG.info("------------------ {} show env begin------------------",simpleName);
		env.forEach((k,v)->{
			LOG.info("{} ==== {}",k,v);
		});
		LOG.info("------------------ {} show env end------------------",simpleName);
		// startup checks and logging
		//提供对JVM执行环境的访问的实用程序类（如执行用户）打印配置信息等
		EnvironmentInformation.logEnvironmentInfo(LOG, StandaloneSessionClusterEntrypoint.class.getSimpleName(), args);
		SignalHandler.register(LOG);
		JvmShutdownSafeguard.installAsShutdownHook(LOG);

		final EntrypointClusterConfiguration entrypointClusterConfiguration = ClusterEntrypointUtils.parseParametersOrExit(
			args,
			new EntrypointClusterConfigurationParserFactory(),
			StandaloneSessionClusterEntrypoint.class);
		//解析配置参数
		Configuration configuration = loadConfiguration(entrypointClusterConfiguration);
		//构造StandaloneSessionClusterEntrypoint,用于启动standalone模式的JM
		StandaloneSessionClusterEntrypoint entrypoint = new StandaloneSessionClusterEntrypoint(configuration);
		//启动集群
		ClusterEntrypoint.runClusterEntrypoint(entrypoint);
	}
}

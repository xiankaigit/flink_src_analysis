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

package org.apache.flink.yarn.entrypoint;

import org.apache.commons.lang3.StringUtils;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.entrypoint.ClusterEntrypoint;
import org.apache.flink.runtime.entrypoint.ClusterEntrypointUtils;
import org.apache.flink.runtime.entrypoint.DynamicParametersConfigurationParserFactory;
import org.apache.flink.runtime.entrypoint.SessionClusterEntrypoint;
import org.apache.flink.runtime.entrypoint.component.DefaultDispatcherResourceManagerComponentFactory;
import org.apache.flink.runtime.entrypoint.component.DispatcherResourceManagerComponentFactory;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.runtime.util.JvmShutdownSafeguard;
import org.apache.flink.runtime.util.SignalHandler;
import org.apache.flink.util.Preconditions;
import org.apache.flink.yarn.configuration.YarnConfigOptions;

import org.apache.hadoop.yarn.api.ApplicationConstants;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

/**
 * Entry point for Yarn session clusters.
 */
public class YarnSessionClusterEntrypoint extends SessionClusterEntrypoint {

	public YarnSessionClusterEntrypoint(Configuration configuration) {
		super(configuration);
	}

	@Override
	protected String getRPCPortRange(Configuration configuration) {
		return configuration.getString(YarnConfigOptions.APPLICATION_MASTER_PORT);
	}

	@Override
	protected DispatcherResourceManagerComponentFactory createDispatcherResourceManagerComponentFactory(Configuration configuration) {
		return DefaultDispatcherResourceManagerComponentFactory.createSessionComponentFactory(YarnResourceManagerFactory.getInstance());
	}


	public static void main(String[] args) {
		String arg_param= StringUtils.join(args, ",");
		String className = Thread.currentThread().getStackTrace()[1].getClassName();
		int idx = className.lastIndexOf(".")+1;
		String simpleName =className.substring(idx);
		System.out.println(simpleName + " commond line is "+arg_param);
		LOG.info(simpleName+" commond line is "+arg_param);
		// startup checks and logging
		EnvironmentInformation.logEnvironmentInfo(LOG, YarnSessionClusterEntrypoint.class.getSimpleName(), args);
		SignalHandler.register(LOG);
		JvmShutdownSafeguard.installAsShutdownHook(LOG);
		Map<String, String> env = System.getenv();
		Map<String,String> newEnv = new HashMap<>(env);
		newEnv.put("FLINK_BIN_DIR","/home/xiankai/apps/flink-1.12.0/bin");
		newEnv.put("FLINK_CONF_DIR","/home/xiankai/apps/flink-1.12.0/conf");
		newEnv.put("FLINK_PLUGINS_DIR","/home/xiankai/apps/flink-1.12.0/plugins");
		newEnv.put("FLINK_LIB_DIR","/home/xiankai/apps/flink-1.12.0/lib");
		newEnv.put("FLINK_OPT_DIR","/home/xiankai/apps/flink-1.12.0/opt");
		newEnv.put("MAX_LOG_FILE_NUMBER","10");
		newEnv.put("HADOOP_USER_NAME","xiankai");
		newEnv.put("PWD","/home/xiankai/apps/flink-1.12.0/conf");
		env=newEnv;


		LOG.info("------------------ {} show env begin------------------",simpleName);
		env.forEach((k,v)->{
			LOG.info("{} ==== {}",k,v);
		});
		LOG.info("------------------ {} show env end------------------",simpleName);


		final String workingDirectory = env.get(ApplicationConstants.Environment.PWD.key());
		Preconditions.checkArgument(
			workingDirectory != null,
			"Working directory variable (%s) not set",
			ApplicationConstants.Environment.PWD.key());

		try {
			YarnEntrypointUtils.logYarnEnvironmentInformation(env, LOG);
		} catch (IOException e) {
			LOG.warn("Could not log YARN environment information.", e);
		}

		final Configuration dynamicParameters = ClusterEntrypointUtils.parseParametersOrExit(
			args,
			new DynamicParametersConfigurationParserFactory(),
			YarnSessionClusterEntrypoint.class);
		final Configuration configuration = YarnEntrypointUtils.loadConfiguration(workingDirectory, dynamicParameters, env);

		YarnSessionClusterEntrypoint yarnSessionClusterEntrypoint = new YarnSessionClusterEntrypoint(configuration);

		ClusterEntrypoint.runClusterEntrypoint(yarnSessionClusterEntrypoint);
	}
}

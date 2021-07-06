package org.apache.flink.runtime.entrypoint.yarn.session;

import org.apache.commons.collections.map.UnmodifiableMap;
import org.apache.flink.ext.ShellExecute;
import org.apache.flink.runtime.entrypoint.StandaloneSessionClusterEntrypoint;
import org.apache.flink.yarn.entrypoint.YarnSessionClusterEntrypoint;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class JmYarnSessionRunTest {

	public static void main(String[] args) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {

//		String jmParam  = "--configDir,/home/xiankai/apps/flink-1.12.0/conf,-D,jobmanager.memory.off-heap.size=134217728b,-D,jobmanager.memory.jvm-overhead.min=201326592b,-D,jobmanager.memory.jvm-metaspace.size=268435456b,-D,jobmanager.memory.heap.size=469762048b,-D,jobmanager.memory.jvm-overhead.max=201326592b";
//		String[] jmArgs = jmParam.split(",");
//		YarnSessionClusterEntrypoint.main(jmArgs);

//		Map<String, String> env = System.getenv();
//		System.out.println("------------------  show env begin------------------");
//		env.forEach((k,v)->{
//			System.out.println(k + " ==== " +v);
//		});




		System.out.println("------------------ show env end------------------");

//		System.out.println("------------------  show prop begin------------------");
//		Properties properties = System.getProperties();
//		properties.forEach((k,v)->{
//			System.out.println(k + " ==== " +v);
//		});
//		System.out.println("------------------ show prop end------------------");
	}

}

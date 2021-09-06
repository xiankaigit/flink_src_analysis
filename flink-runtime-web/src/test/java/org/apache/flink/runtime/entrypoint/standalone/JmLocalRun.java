package org.apache.flink.runtime.entrypoint.standalone;

import org.apache.flink.runtime.entrypoint.StandaloneSessionClusterEntrypoint;

public class JmLocalRun {

	public static void main(String[] args) {
		String jmParam  = "--configDir,/home/hik/apps/flink-1.12.0/conf,--executionMode,cluster,-D,jobmanager.memory.off-heap.size=134217728b,-D,jobmanager.memory.jvm-overhead.min=201326592b,-D,jobmanager.memory.jvm-metaspace.size=268435456b,-D,jobmanager.memory.heap.size=1073741824b,-D,jobmanager.memory.jvm-overhead.max=201326592b";
		String[] jmArgs = jmParam.split(",");
		StandaloneSessionClusterEntrypoint.main(jmArgs);
	}

}

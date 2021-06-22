package org.apache.flink.runtime;

import org.apache.flink.runtime.entrypoint.StandaloneSessionClusterEntrypoint;
import org.apache.flink.runtime.taskexecutor.TaskManagerRunner;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

public class LocalRunTest extends TestLogger {

	public static void main(String[] args) throws Exception {
		String jmParam  = "--configDir,/mnt/disk1/dev/flink-1.12.0/conf,--executionMode,cluster,-D,jobmanager.memory.off-heap.size=134217728b,-D,jobmanager.memory.jvm-overhead.min=201326592b,-D,jobmanager.memory.jvm-metaspace.size=268435456b,-D,jobmanager.memory.heap.size=1073741824b,-D,jobmanager.memory.jvm-overhead.max=201326592b";
		String[] jmArgs = jmParam.split(",");
		StandaloneSessionClusterEntrypoint.main(jmArgs);

		String tmParam = "--configDir,/mnt/disk1/dev/flink-1.12.0/conf,-D,taskmanager.memory.framework.off-heap.size=134217728b,-D,taskmanager.memory.network.max=134217730b,-D,taskmanager.memory.network.min=134217730b,-D,taskmanager.memory.framework.heap.size=134217728b,-D,taskmanager.memory.managed.size=536870920b,-D,taskmanager.cpu.cores=1.0,-D,taskmanager.memory.task.heap.size=402653174b,-D,taskmanager.memory.task.off-heap.size=0b";
		String[] tmArgs = tmParam.split(",");

		TaskManagerRunner.main(tmArgs);
	}

	@Test
	public void testJmRun() throws Exception {
		String jmParam  = "--configDir,/mnt/disk1/dev/flink-1.12.0/conf,--executionMode,cluster,-D,jobmanager.memory.off-heap.size=134217728b,-D,jobmanager.memory.jvm-overhead.min=201326592b,-D,jobmanager.memory.jvm-metaspace.size=268435456b,-D,jobmanager.memory.heap.size=1073741824b,-D,jobmanager.memory.jvm-overhead.max=201326592b";
		String[] jmArgs = jmParam.split(",");
		StandaloneSessionClusterEntrypoint.main(jmArgs);
	}
	@Test
	public void testTmRun() throws Exception {
		String tmParam = "--configDir,/mnt/disk1/dev/flink-1.12.0/conf,-D,taskmanager.memory.framework.off-heap.size=134217728b,-D,taskmanager.memory.network.max=134217730b,-D,taskmanager.memory.network.min=134217730b,-D,taskmanager.memory.framework.heap.size=134217728b,-D,taskmanager.memory.managed.size=536870920b,-D,taskmanager.cpu.cores=1.0,-D,taskmanager.memory.task.heap.size=402653174b,-D,taskmanager.memory.task.off-heap.size=0b";
		String[] tmArgs = tmParam.split(",");
		TaskManagerRunner.main(tmArgs);
	}



}

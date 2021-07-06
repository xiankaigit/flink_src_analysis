package org.apache.flink.runtime.entrypoint.standalone;

import org.apache.flink.runtime.taskexecutor.TaskManagerRunner;

public class TmLocalRun {

	public static void main(String[] args) throws Exception {
		String tmParam = "--configDir,/mnt/disk1/dev/flink-1.12.0/conf,-D,taskmanager.memory.framework.off-heap.size=134217728b,-D,taskmanager.memory.network.max=134217730b,-D,taskmanager.memory.network.min=134217730b,-D,taskmanager.memory.framework.heap.size=134217728b,-D,taskmanager.memory.managed.size=536870920b,-D,taskmanager.cpu.cores=1.0,-D,taskmanager.memory.task.heap.size=402653174b,-D,taskmanager.memory.task.off-heap.size=0b";
		String[] tmArgs = tmParam.split(",");
		TaskManagerRunner.main(tmArgs);
	}

}

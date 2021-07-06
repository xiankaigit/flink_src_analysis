package org.apache.flink.runtime.entrypoint.yarn.perjob;

import org.apache.flink.client.cli.CliFrontend;

public class PerJobCliTest {

    public static void main(String[] args) {
        //需要在flink-conf.yml中添加配置 yarn.flink-dist-jar: /home/xiankai/apps/flink-1.12.0/lib/flink-dist_2.11-1.12.0.jar
        String cmd = "run,-m,yarn-cluster,/home/xiankai/apps/flink-1.12.0/examples/streaming/WordCount.jar";

        String[] cliArgs = cmd.split(",");
        CliFrontend.main(cliArgs);

    }

}

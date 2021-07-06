package org.apache.flink.runtime.entrypoint.yarn.session;

import org.apache.flink.yarn.cli.FlinkYarnSessionCli;

public class FlinkYarnSessionCliTest {

    public static void main(String[] args) {

        String cmd = "-j,/home/xiankai/apps/flink-1.12.0/lib/flink-dist_2.11-1.12.0.jar,-jm,1024m,-tm,2048m,-d,true";
        String[] cliArgs = cmd.split(",");
        FlinkYarnSessionCli.main(cliArgs);

    }

}

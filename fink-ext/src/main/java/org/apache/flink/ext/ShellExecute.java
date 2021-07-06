package org.apache.flink.ext;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;


/**
 * @Description:
 * @Author: xiankai
 * @CreateDate: 2019/5/28 16:53
 */
public class ShellExecute {

    protected static final Logger LOG = LoggerFactory.getLogger(ShellExecute.class);


    private static Integer SUCCESS = 0;


    public static Boolean runCommand(String cmd) {
        try {
            Process proc = Runtime.getRuntime().exec(cmd);
            GobblerThread errorGobbler = new GobblerThread(
                    proc.getErrorStream(), "ERROR");
            GobblerThread outputGobbler = new GobblerThread(
                    proc.getInputStream(), "OUTPUT");
            errorGobbler.start();
            outputGobbler.start();
            int exitVal = proc.waitFor();
            LOG.info("ExitValue: " + exitVal);
            return true;
        } catch (Exception e) {
            LOG.error("execute cmd " + cmd + " error: ",e);
            return false;
        }

    }


    /**
     * @author xiankai
     * @date 2019/5/28 16:53
     */
    public static Boolean runCommand2(String command) {
        Boolean success = null;
        LOG.info("begin execute command {}", command);
        Integer status = null;
        Process pro = null;
        try {
            Runtime runtime = Runtime.getRuntime();
            LOG.info("=============== before =================");
            pro = runtime.exec(command);
            LOG.info("=============== wait =================");
            status = pro.waitFor();
            LOG.info("=============== end =================");
        } catch (InterruptedException e) {
            LOG.error("execute command occur InterruptedException", e);
            success = false;
        } catch (IOException e) {
            LOG.error("execute command occur IOException", e);
            success = false;
        }

        LOG.info("executor finished, status is {}", status);
        if (pro == null) {
            LOG.error("get execute process error");
            success = false;
        }
        LOG.info("-------------------------- print execute log --------------------------");

        BufferedReader input = new BufferedReader(new InputStreamReader(pro.getInputStream()));
        String line = null;
        try {
            while ((line = input.readLine()) != null) {
                LOG.info(line);
            }
            input.close();
        } catch (IOException e) {
            LOG.error("print execute log occur error", e);
        }finally {
            // pro.destroy();
        }
        LOG.info("------------------------ print execute log end ------------------------");
        LOG.info("execute finished command {}", command);
        if (success != null && !success)
            return false;
        return SUCCESS == status;
    }



    /*public String submit(DataCollectConfig dataCollectConfig) {
        String command = null;
        try {
            command = submitCommandLineUtil.crateCommandLine(dataCollectConfig);
        } catch (IllegalAccessException e) {
            LOGGER.error("create command error", e);
            return "create command error";
        }

        Integer status = null;
        Process pro = null;
        try {
            Runtime runtime = Runtime.getRuntime();
            pro = runtime.exec(command);
            status = pro.waitFor();
        } catch (InterruptedException e) {
            LOGGER.error("submit to flink occur InterruptedException", e);
            return "submit to flink error " + e.getMessage();
        } catch (IOException e) {
            LOGGER.error("submit to flink occur IOException", e);
            return "submit to flink error " + e.getMessage();
        }


        LOGGER.info("executor finished, status is {}", status);
        if (pro == null) {
            LOGGER.error("get execute process error");
            return "get execute process error";
        }
        LOGGER.info("-------------------------- print execute log --------------------------");

        BufferedReader input = new BufferedReader(new InputStreamReader(pro.getInputStream()));
        StringBuffer sb = new StringBuffer();
        String line;
        try {
            while ((line = input.readLine()) != null) {
                LOGGER.info(line);
            }
            input.close();
        } catch (IOException e) {
            LOGGER.error("print execute log occur error", e);
            return "print execute log occur error";
        }
        LOGGER.info("------------------------ print execute log end ------------------------");

        LOGGER.info("submit job {} to flink success", dataCollectConfig.getName());
        return CodeAndMsgConstants.Message.SUCCESS;
    }*/


    public static void main(String[] args) {
        String cmd = "/opt/opsmgr/web/components/xfusion.1/bin/xfusion-stream/lib/../run/initJobSubmitEnv.sh HIK_SMART_METADATA_COLLECT hdh62,hdh63,hdh64,hdh65,hdh66,hdh88,hdh67,hdh89,hdh68,hdh69,hdb60,hdh61,hdh51,hdh73,hdh52,hdh74,hdh53,hdh75,hdh54,hdh76,hdh55,hdh77,hdh70,hdh71,hdh72,hdh59";
        Boolean aBoolean = ShellExecute.runCommand(cmd);
    }
}

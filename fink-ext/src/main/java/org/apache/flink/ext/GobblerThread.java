package org.apache.flink.ext;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 * Author: xiankai
 * Date: 2019/10/15 16:33
 */
public class GobblerThread extends Thread {

    protected static final Logger LOG = LoggerFactory.getLogger(GobblerThread.class);


    private InputStream is;
    private String type;

    GobblerThread(InputStream is, String type) {
        this.is = is;
        this.type = type;
    }

    public void run() {
        try {
            InputStreamReader isr = new InputStreamReader(is);
            BufferedReader br = new BufferedReader(isr);
            String line = null;
            while ((line = br.readLine()) != null)
                LOG.info(line);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

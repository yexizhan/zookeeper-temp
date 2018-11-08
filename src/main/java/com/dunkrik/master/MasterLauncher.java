package com.dunkrik.master;

import com.dunkrik.util.ApplicationConfig;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.Random;

public class MasterLauncher {

    public static void main(String[] args) throws IOException, InterruptedException {
        String hostPort = ApplicationConfig.getProperty("hostPort");
        Integer sessionTimeout = ApplicationConfig.getProperty("sessionTimeout") == null ? 0 : Integer.valueOf(ApplicationConfig.getProperty("sessionTimeout"));
        MasterNode masterNode = new MasterNode(Integer.toString((new Random()).nextInt()));
        ZooKeeper zooKeeper = new ZooKeeper(hostPort, sessionTimeout, masterNode);
        masterNode.setZooKeeper(zooKeeper);
        MetaNode metaNode = new MetaNode(zooKeeper);
        MasterListener masterListener = new MasterListener(metaNode);
        masterNode.addObserver(masterListener);
        masterNode.runForMaster();
        Thread.sleep(60000);
        zooKeeper.close();
    }


}

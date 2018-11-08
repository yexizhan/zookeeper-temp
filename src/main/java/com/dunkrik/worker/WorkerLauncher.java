package com.dunkrik.worker;

import com.dunkrik.util.ApplicationConfig;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.Random;

public class WorkerLauncher {

    public static void main(String[] args) throws IOException, InterruptedException {
        String hostPort = ApplicationConfig.getProperty("hostPort");
        Integer sessionTimeout = ApplicationConfig.getProperty("sessionTimeout") == null ? 0 : Integer.valueOf(ApplicationConfig.getProperty("sessionTimeout"));
        Worker worker = new Worker(Integer.toHexString((new Random()).nextInt()));
        ZooKeeper zooKeeper = new ZooKeeper(hostPort, sessionTimeout, worker);
        worker.setZooKeeper(zooKeeper);
        WorkerListener workerListener = new WorkerListener();
        worker.addObserver(workerListener);
        worker.register();
        Thread.sleep(60000);
    }
}

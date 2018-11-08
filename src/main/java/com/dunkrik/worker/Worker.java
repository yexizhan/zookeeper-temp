package com.dunkrik.worker;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Observable;
import java.util.Random;

public class Worker extends Observable implements Watcher {

    private ZooKeeper zooKeeper;

    public Worker(String workerId) {
        this.workerId = workerId;
    }

    private String workerId;

    private String status;

    @Override
    public void process(WatchedEvent event) {
        System.out.println(event.toString());
    }

    public void register() {
        zooKeeper.create("/workers/worker-" + getWorkerId(),
                "Idle".getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL,
                createWorkerCallback,
                null);
    }

    AsyncCallback.StringCallback createWorkerCallback = new AsyncCallback.StringCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    register();
                    break;
                case OK:
                    System.out.println("Registered worker successfully " + getWorkerId());
                    setChanged();
                    notifyObservers();
                    break;
                case NODEEXISTS:
                    System.out.println("Already registered " + getWorkerId());
                    break;
                default:
                    System.out.println("Something went wrong: " + KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }
    };

    private String getWorkerId() {
        return workerId;
    }

    private String getStatus() {
        return status;
    }

    public void setZooKeeper(ZooKeeper zooKeeper) {
        this.zooKeeper = zooKeeper;
    }

    private void setStatusParams(String status) {
        this.status = status;
    }

    private synchronized void updateStatus(String status) {
        if (status == getStatus()) {
            zooKeeper.setData("/workers/worker-" + getWorkerId(),
                    status.getBytes(), -1,
                    statusUpdateCallback, status);
        }
    }

    public void setStatus(String status) {
        this.setStatusParams(status);
        updateStatus(status);
    }

    AsyncCallback.StatCallback statusUpdateCallback = new AsyncCallback.StatCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, Stat stat) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    updateStatus((String) ctx);
                    return;
            }
        }
    };
}

package com.dunkrik.old;


import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Random;

public class Master implements Watcher {

    ZooKeeper zk;
    String hostPort;
    String serverId = Integer.toString((new Random()).nextInt());
    boolean isLeader = false;

    public Master(String hostPort) {
        this.hostPort = hostPort;
    }

    void startZK() throws IOException {
        zk = new ZooKeeper(hostPort, 15000, this);
    }

    void stopZK() throws InterruptedException {
        zk.close();
    }

    boolean checkMaster() throws InterruptedException, KeeperException {
        while (true) {
            try {
                Stat stat = new Stat();
                byte data[] = zk.getData("/master", false, stat);
                isLeader = new String(data).equals(serverId);
            } catch (KeeperException e) {
                if (e instanceof KeeperException.NoNodeException) {
                    return false;
                } else if (e instanceof KeeperException.ConnectionLossException) {

                } else {
                    throw e;
                }
            }
        }
    }

    void runForMaster() throws KeeperException, InterruptedException {
        while (true) {
            try {
                zk.create("/master", serverId.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                isLeader = true;
                break;
            } catch (KeeperException e) {
                if (e instanceof KeeperException.NodeExistsException) {
                    isLeader = false;
                    break;
                } else if (e instanceof KeeperException.ConnectionLossException) {

                } else {
                    throw e;
                }
            }
            if(checkMaster()) {
                break;
            }
        }
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        System.out.println(watchedEvent);
    }

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        Master m = new Master("192.168.142.128:2181");
        m.startZK();
        m.runForMaster();
        if (m.isLeader) {
            System.out.println("master");
            Thread.sleep(60000L);
        } else {
            System.out.println("not master");
        }

        m.stopZK();
    }
}
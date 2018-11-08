package com.dunkrik.old;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Random;

public class AsyncMaster implements Watcher {
    ZooKeeper zk;
    String hostPort;
    String serverId = Integer.toString((new Random()).nextInt());
    boolean isLeader = false;

    public AsyncMaster(String hostPort) {
        this.hostPort = hostPort;
    }

    void startZK() throws IOException {
        zk = new ZooKeeper(hostPort, 15000, this);
    }

    void stopZK() throws InterruptedException {
        zk.close();
    }

    AsyncCallback.StringCallback masterCreateCallback = new AsyncCallback.StringCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    checkMaster();
                    return;
                case OK:
                    isLeader = true;
                    break;
                default:
                    isLeader = false;
            }
            System.out.println("I'm " + (isLeader ? "" : "not ") + "the leader");
        }
    };

    AsyncCallback.DataCallback masterCheckCallback = new AsyncCallback.DataCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    checkMaster();
                    return;
                case NONODE:
                    runForMaster();
                    return;
            }
        }
    };

    void checkMaster() {
        zk.getData("/master", false, masterCheckCallback, null);
    }

    void runForMaster() {
        zk.create("/master", serverId.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, masterCreateCallback, null);
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        System.out.println(watchedEvent);
    }

    void createParent(String path, byte[] data) {
        zk.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, createParentCallback, data);
    }

    AsyncCallback.StringCallback createParentCallback = new AsyncCallback.StringCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    createParent(path, (byte[]) ctx);
                    break;
                case OK:
                    System.out.println("Parent created");
                    break;
                case NODEEXISTS:
                    System.err.println("Parent already registered:" + path);
                    break;
                default:
                    System.err.println("Something went wrong: " + KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }
    };

    public void bootstrap() {
        createParent("/workers", new byte[0]);
        createParent("/assign", new byte[0]);
        createParent("/tasks", new byte[0]);
        createParent("/status", new byte[0]);
    }

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        AsyncMaster m = new AsyncMaster("192.168.142.128:2181,192.168.142.128:2182,192.168.142.128:2183");
        m.startZK();
        m.runForMaster();
        m.bootstrap();
        Thread.sleep(60000);
        m.stopZK();
    }
}

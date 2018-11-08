package com.dunkrik.client;

import com.dunkrik.util.ApplicationConfig;
import org.apache.zookeeper.*;

import java.io.IOException;

public class ZkClient implements Watcher {
    @Override
    public void process(WatchedEvent event) {
        System.out.println(event);
    }

    ZooKeeper zooKeeper;
    String hostPort = ApplicationConfig.getProperty("hostPort");
    Integer sessionTimeout = ApplicationConfig.getProperty("sessionTimeout") == null ? 0 : Integer.valueOf(ApplicationConfig.getProperty("sessionTimeout"));

    void startZK() throws IOException {
        zooKeeper = new ZooKeeper(hostPort,sessionTimeout,this);
    }

    String queueCommand(String command) throws Exception {
        String name = "";
        while(true) {
            try {
                name = zooKeeper.create("/tasks/task-",
                        command.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
                break;
            } catch ( KeeperException.NodeExistsException e) {
                throw new Exception( name + " already appears to be running");
            } catch (KeeperException.ConnectionLossException e) {

            }
        }
        return name;
    }

    public static void main(String args[]) throws Exception {
        ZkClient c = new ZkClient();
        c.startZK();
        String name = c.queueCommand("fucklzf");
        System.out.println("Created " + name);
    }
}

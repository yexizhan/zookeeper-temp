package com.dunkrik.master;

import org.apache.zookeeper.*;

public class MetaNode {

    private ZooKeeper zooKeeper;

    public MetaNode(ZooKeeper zooKeeper) {
        this.zooKeeper = zooKeeper;
    }

    public void createParent(String path, byte[] data) {
        zooKeeper.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, createParentCallback, data);
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
}

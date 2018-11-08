package com.dunkrik.master;

import com.dunkrik.util.ChildrenCache;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Observable;

public class MasterNode extends Observable implements Watcher {

    private static final Logger LOG = LoggerFactory.getLogger(MasterNode.class);

    /*
     * A master process can be either running for
     * primary master, elected primary master, or
     * not elected, in which case it is a backup
     * master.
     */
    enum MasterStates {
        RUNNING, ELECTED, NOTELECTED
    }

    ;

    ChildrenCache workersCache;

    private volatile MasterStates state = MasterStates.RUNNING;

    MasterStates getState() {
        return state;
    }

    private boolean isLeader = false;
    private String serverId;
    private ZooKeeper zooKeeper;

    public MasterNode(String serverId) {
        this.serverId = serverId;
    }

    @Override
    public void process(WatchedEvent event) {
        System.out.println(event);
    }

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

    public void checkMaster() {
        zooKeeper.getData("/master", false, masterCheckCallback, null);
    }

    public void runForMaster() {
        zooKeeper.create("/master", serverId.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, masterCreateCallback, null);
    }

    AsyncCallback.StringCallback masterCreateCallback = new AsyncCallback.StringCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    checkMaster();
                    return;
                case OK:
                    setLeader(true);
                    state = MasterStates.ELECTED;
                    break;
                case NODEEXISTS:
                    masterExists();
                    state = MasterStates.NOTELECTED;
                    break;
                default:
                    state = MasterStates.NOTELECTED;
                    setLeader(false);
                    LOG.error("Something went wrong when running for master.", KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }
    };

    private void masterExists() {
        zooKeeper.exists("/master", masterExistsWatcher, masterExistsCallback, null);
    }

    Watcher masterExistsWatcher = new Watcher() {
        @Override
        public void process(WatchedEvent event) {
            if (event.getType() == Event.EventType.NodeDeleted) {
                assert "/master".equals(event.getPath());
                runForMaster();
            }
        }
    };

    AsyncCallback.StatCallback masterExistsCallback = new AsyncCallback.StatCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, Stat stat) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    masterExists();
                    break;
                case OK:
                    if (stat == null) {
                        state = MasterStates.RUNNING;
                        runForMaster();
                    }
                    break;
                default:
                    checkMaster();
                    break;
            }
        }
    };

    void takeLeadership() {
        LOG.info("Going for list of workers");
        getWorkers();
        //TODO
    }


    Watcher workersChangeWatcher = new Watcher() {
        @Override
        public void process(WatchedEvent event) {
            if (event.getType() == Event.EventType.NodeChildrenChanged) {
                assert "/workers".equals(event.getPath());
                getWorkers();
            }
        }
    };

    void getWorkers() {
        zooKeeper.getChildren("/workers", workersChangeWatcher, workersGetChildrenCallback, null);
    }

    AsyncCallback.ChildrenCallback workersGetChildrenCallback = new AsyncCallback.ChildrenCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, List<String> children) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    getWorkers();
                    break;
                case OK:
                    LOG.info("Successfully got a list of workers:" + children.size() + " workers");
                    reassignAndSet(children);
                    break;
                default:
                    LOG.error("getChildren failed", KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }
    };

    void reassignAndSet(List<String> children) {
        List<String> toProcess;
        if (workersCache == null) {
            workersCache = new ChildrenCache(children);
            toProcess = null;
        } else {
            LOG.info("Removing and setting");
            toProcess = workersCache.removeAndSet(children);
        }

        if (toProcess != null) {
            for (String worker : toProcess) {
                //TODO
            }
        }
    }

    public boolean isLeader() {
        return isLeader;
    }

    public void setLeader(boolean leader) {
        isLeader = leader;
        setChanged();
        notifyObservers();
    }

    public void setZooKeeper(ZooKeeper zooKeeper) {
        this.zooKeeper = zooKeeper;
    }
}

package com.dunkrik.master;

import java.util.Observable;
import java.util.Observer;

public class MasterListener implements Observer {

    private MetaNode metaNode;

    public MasterListener(MetaNode metaNode) {
        this.metaNode = metaNode;
    }

    @Override
    public void update(Observable o, Object arg) {
        MasterNode masterNode = (MasterNode)o;
        boolean isLeader = masterNode.isLeader();
        if(isLeader) {
            this.metaNode.createParent("/workers", new byte[0]);
            this.metaNode.createParent("/assign", new byte[0]);
            this.metaNode.createParent("/tasks", new byte[0]);
            this.metaNode.createParent("/status", new byte[0]);
        }
    }
}

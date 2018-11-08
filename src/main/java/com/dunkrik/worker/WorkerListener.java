package com.dunkrik.worker;

import com.dunkrik.worker.Worker;

import java.util.Observable;
import java.util.Observer;

public class WorkerListener implements Observer {

    @Override
    public void update(Observable o, Object arg) {
        Worker worker = (Worker)o;
        worker.setStatus("Busy");
    }
}

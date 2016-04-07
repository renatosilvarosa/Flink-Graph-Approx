package pt.tecnico.graph.job;

import java.io.Serializable;

/**
 * Created by Renato on 30/03/2016.
 */
public class Configuration implements Serializable {
    private long maximumTime;
    private int maximumUpdates;

    public Configuration() {
    }

    public Configuration(long maximumTime, int maximumUpdates) {
        this.maximumTime = maximumTime;
        this.maximumUpdates = maximumUpdates;
    }

    public long getMaximumTime() {
        return maximumTime;
    }

    public void setMaximumTime(long maximumTime) {
        this.maximumTime = maximumTime;
    }

    public int getMaximumUpdates() {
        return maximumUpdates;
    }

    public void setMaximumUpdates(int maximumUpdates) {
        this.maximumUpdates = maximumUpdates;
    }
}
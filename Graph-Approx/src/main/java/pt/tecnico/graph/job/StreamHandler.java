package pt.tecnico.graph.job;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Graph;
import org.apache.flink.types.NullValue;
import pt.tecnico.graph.stream.StreamProvider;

import java.util.concurrent.BlockingQueue;

/**
 * Created by Renato on 25/05/2016.
 */
public abstract class StreamHandler implements Runnable {
    protected final BlockingQueue<String> pendingUpdates;
    protected final ExecutionEnvironment env;
    private final StreamProvider<String> updateStream;
    protected Graph<Long, NullValue, NullValue> graph;

    public StreamHandler(StreamProvider<String> updateStream, Graph<Long, NullValue, NullValue> graph) {
        this.updateStream = updateStream;
        this.pendingUpdates = updateStream.getQueue();
        this.graph = graph;
        this.env = graph.getContext();
    }

    public void start() {
        new Thread(updateStream, "Stream thread").start();
        new Thread(this, "Update Graph Thread").start();
    }
}

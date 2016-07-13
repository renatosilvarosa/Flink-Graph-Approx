package pt.tecnico.graph.stream;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Graph;
import org.apache.flink.types.NullValue;
import pt.tecnico.graph.output.DiscardingGraphOutputFormat;
import pt.tecnico.graph.output.GraphOutputFormat;

import java.util.concurrent.BlockingQueue;

/**
 * Created by Renato on 25/05/2016.
 */
public abstract class GraphStreamHandler<R> implements Runnable {
    protected final BlockingQueue<String> pendingUpdates;
    protected final ExecutionEnvironment env;
    private final StreamProvider<String> updateStream;
    protected Graph<Long, NullValue, NullValue> graph;
    protected GraphOutputFormat<R> outputFormat;

    public GraphStreamHandler(StreamProvider<String> updateStream, Graph<Long, NullValue, NullValue> graph) {
        this.updateStream = updateStream;
        this.pendingUpdates = updateStream.getQueue();
        this.graph = graph;
        this.env = graph.getContext();
        this.outputFormat = new DiscardingGraphOutputFormat<>();
    }

    public void start() {
        new Thread(updateStream, "Stream thread").start();
        new Thread(this, "Update Graph Thread").start();
    }

    public void setOutputFormat(GraphOutputFormat<R> outputFormat) {
        this.outputFormat = outputFormat;
    }
}

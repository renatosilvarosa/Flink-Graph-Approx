package pt.tecnico.graph.stream;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.TypeSerializerInputFormat;
import org.apache.flink.api.java.io.TypeSerializerOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import pt.tecnico.graph.output.DiscardingGraphOutputFormat;
import pt.tecnico.graph.output.GraphOutputFormat;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.stream.Collectors;

/**
 * Created by Renato on 25/05/2016.
 */
public abstract class GraphStreamHandler<R> implements Runnable {
    protected final BlockingQueue<String> pendingUpdates;
    protected final ExecutionEnvironment env;
    private final StreamProvider<String> updateStream;
    protected Graph<Long, NullValue, NullValue> graph;
    protected GraphOutputFormat<R> outputFormat;
    protected GraphUpdateTracker<Long, NullValue, NullValue> graphUpdateTracker;
    protected TypeSerializerInputFormat<Tuple2<Long, Long>> edgeInputFormat;
    protected TypeSerializerOutputFormat<Tuple2<Long, Long>> edgeOutputFormat;
    protected int iteration = 0;

    public GraphStreamHandler(StreamProvider<String> updateStream, Graph<Long, NullValue, NullValue> graph) {
        this.updateStream = updateStream;
        this.pendingUpdates = updateStream.getQueue();
        this.graph = graph;
        this.env = graph.getContext();
        this.outputFormat = new DiscardingGraphOutputFormat<>();
    }

    public void start() throws Exception {
        init();
        new Thread(updateStream, "Stream thread").start();
        new Thread(this, "Update Graph Thread").start();
    }

    public void setOutputFormat(GraphOutputFormat<R> outputFormat) {
        this.outputFormat = outputFormat;
    }

    public abstract void init() throws Exception;

    protected void applyUpdates() throws Exception {
        edgeInputFormat.setFilePath("cache/edges" + ((iteration - 1) % 5));
        graph = Graph.fromTuple2DataSet(env.createInput(edgeInputFormat), env);

        if (!graphUpdateTracker.getVerticesToAdd().isEmpty()) {
            List<Vertex<Long, NullValue>> vertices = graphUpdateTracker.getVerticesToAdd().stream()
                    .map(id -> new Vertex<>(id, NullValue.getInstance()))
                    .distinct()
                    .collect(Collectors.toList());

            graph = graph.addVertices(vertices);
        }

        if (!graphUpdateTracker.getEdgesToAdd().isEmpty()) {
            graph = graph.addEdges(new ArrayList<>(graphUpdateTracker.getEdgesToAdd()));
        }

        if (!graphUpdateTracker.getVerticesToRemove().isEmpty()) {
            graph = graph.removeVertices(graphUpdateTracker.getVerticesToRemove().stream()
                    .map(id -> new Vertex<>(id, NullValue.getInstance()))
                    .distinct()
                    .collect(Collectors.toList()));
        }

        if (!graphUpdateTracker.getEdgesToRemove().isEmpty()) {
            graph = graph.removeEdges(new ArrayList<>(graphUpdateTracker.getEdgesToRemove()));
        }

        edgeOutputFormat.setOutputFilePath(new Path("cache/edges" + (iteration % 5)));
        graph.getEdgeIds().output(edgeOutputFormat);

        env.execute("Apply updates it. " + iteration);
        graphUpdateTracker.resetUpdates();
    }

    protected void registerEdgeDelete(String[] split) {
        Edge<Long, NullValue> edge = parseEdge(split);
        graphUpdateTracker.removeEdge(edge);
    }

    protected void registerEdgeAdd(String[] split) {
        Edge<Long, NullValue> edge = parseEdge(split);
        graphUpdateTracker.addEdge(edge);
    }

    private Edge<Long, NullValue> parseEdge(String[] data) {
        assert data.length == 3;
        return new Edge<>(Long.valueOf(data[1]), Long.valueOf(data[2]), NullValue.getInstance());
    }
}

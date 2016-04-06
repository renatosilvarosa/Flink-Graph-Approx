package pt.tecnico.graph;

import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import pt.tecnico.graph.stream.StreamProvider;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

/**
 * Created by Renato on 30/03/2016.
 */
public class ApproxGraphJob<K, EV, R> implements Serializable {
    private static final Configuration DEFAULT_CONFIGURATION = new Configuration(5000, 1000);

    private final ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
    private final Configuration configuration;

    private Graph<K, NullValue, EV> graph;
    private StreamProvider<Edge<K, EV>> edgeStream;
    private GraphAlgorithm<K, NullValue, EV, DataSet<R>> algorithm;
    private OutputFormat<R> outputFormat;

    private BlockingQueue<Edge<K, EV>> pendingUpdates;

    private ExecutionEnvironment graphEnvironment;
    private final Collection<Edge<K, EV>> accumulator = Collections.synchronizedCollection(new ArrayList<>());
    private ScheduledFuture<?> nextExecution;

    public ApproxGraphJob() {
        this(DEFAULT_CONFIGURATION);
    }

    public ApproxGraphJob(Configuration configuration) {
        this.configuration = configuration;
    }

    public void start() {
        new Thread(edgeStream, "Stream thread").start();
        nextExecution = executorService.schedule(this::updateGraph, configuration.getMaximumTime(), TimeUnit.MILLISECONDS);
        new Thread(() -> {
            while (true) {
                try {
                    Edge<K, EV> update = pendingUpdates.take();
                    accumulator.add(update);

                    if (accumulator.size() >= configuration.getMaximumUpdates()) {
                        updateGraph();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    break;
                }

            }
        }, "Update Graph Thread").start();
    }

    private void updateGraph() {
        if (nextExecution.getDelay(TimeUnit.MILLISECONDS) > 0) {
            nextExecution.cancel(false);
        }

        if (!accumulator.isEmpty()) {
            List<Edge<K, EV>> updates;
            synchronized (accumulator) {
                updates = new ArrayList<>(accumulator);
                accumulator.clear();
            }

            try {
                graph.getContext().startNewSession();
                graph = graph.addVertices(getVertices(updates));
                graph = graph.addEdges(updates);
                DataSet<R> result = graph.run(algorithm);
                result.output(outputFormat);
                graph.getContext().execute();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        nextExecution = executorService.schedule(this::updateGraph, configuration.getMaximumTime(), TimeUnit.MILLISECONDS);
    }

    public void setGraph(Graph<K, NullValue, EV> graph) {
        this.graph = graph;
        graphEnvironment = graph.getContext();
    }

    private List<Vertex<K, NullValue>> getVertices(Collection<Edge<K, EV>> edges) {
        Set<K> ids = new HashSet<>();
        for (Edge<K, EV> e : edges) {
            ids.add(e.getSource());
            ids.add(e.getTarget());
        }
        return ids.stream().map(id -> new Vertex<>(id, NullValue.getInstance())).collect(Collectors.toList());
    }

    public void setEdgeStream(StreamProvider<Edge<K, EV>> edgeStream) {
        this.edgeStream = edgeStream;
        this.pendingUpdates = edgeStream.getQueue();
    }

    public void setAlgorithm(GraphAlgorithm<K, NullValue, EV, DataSet<R>> algorithm) {
        this.algorithm = algorithm;
    }

    public void setOutputFormat(OutputFormat<R> outputFormat) {
        this.outputFormat = outputFormat;
    }
}

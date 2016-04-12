package pt.tecnico.graph.job;

import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
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
 * Created by Renato on 07/04/2016.
 */
public abstract class ApproxGraphGob<K, VV, EV, U, R> implements Serializable {
    public static final String ITERATION = "ApproxGraphGob.iteration";

    protected static final GraphJobConfiguration DEFAULT_CONFIGURATION = new GraphJobConfiguration(5000, 1000);
    protected final GraphJobConfiguration configuration;
    protected final ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();

    protected final Collection<U> accumulator = Collections.synchronizedCollection(new ArrayList<>());

    protected Graph<K, VV, EV> graph;
    protected StreamProvider<U> updateStream;
    protected GraphAlgorithm<K, VV, EV, DataSet<R>> algorithm;

    protected OutputFormat<R> outputFormat;
    protected BlockingQueue<U> pendingUpdates;
    protected ExecutionEnvironment graphEnvironment;
    protected ScheduledFuture<?> nextExecution;
    protected int iteration = 0;

    public ApproxGraphGob(GraphJobConfiguration configuration) {
        this.configuration = configuration;
    }

    protected abstract Graph<K, VV, EV> updateGraph(List<U> updates);

    public void start() {

        try {
            runAlgorithm();
        } catch (Exception e) {
            e.printStackTrace();
        }

        new Thread(updateStream, "Stream thread").start();

        nextExecution = executorService.schedule(this::update, configuration.getMaximumTime(), TimeUnit.MILLISECONDS);

        new Thread(() -> {
            while (true) {
                try {
                    U update = pendingUpdates.take();
                    accumulator.add(update);

                    if (accumulator.size() >= configuration.getMaximumUpdates()) {
                        update();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    break;
                }

            }
        }, "Update Graph Thread").start();
    }

    private void update() {
        if (nextExecution.getDelay(TimeUnit.MILLISECONDS) > 0) {
            nextExecution.cancel(false);
        }

        if (!accumulator.isEmpty()) {
            List<U> updates;
            synchronized (accumulator) {
                updates = new ArrayList<>(accumulator);
                accumulator.clear();
            }

            try {
                graph = updateGraph(updates);
                runAlgorithm();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        nextExecution = executorService.schedule(this::update, configuration.getMaximumTime(), TimeUnit.MILLISECONDS);
    }

    private void runAlgorithm() throws Exception {
        DataSet<R> result = graph.run(algorithm);
        Configuration conf = new Configuration();
        conf.setInteger(ITERATION, iteration);
        result.output(outputFormat).withParameters(conf).setParallelism(1);
        graph.getContext().setParallelism(1);
        graph.getContext().execute();
        iteration++;
    }

    public void setGraph(Graph<K, VV, EV> graph) {
        this.graph = graph;
        graphEnvironment = graph.getContext();
    }

    public void setUpdateStream(StreamProvider<U> stream) {
        this.updateStream = stream;
        this.pendingUpdates = stream.getQueue();
    }

    public void setAlgorithm(GraphAlgorithm<K, VV, EV, DataSet<R>> algorithm) {
        this.algorithm = algorithm;
    }

    public void setOutputFormat(OutputFormat<R> outputFormat) {
        this.outputFormat = outputFormat;
    }

    public List<Vertex<K, NullValue>> getVertices(Collection<Edge<K, EV>> edges) {
        Set<K> ids = new HashSet<>();
        for (Edge<K, EV> e : edges) {
            ids.add(e.getSource());
            ids.add(e.getTarget());
        }
        return ids.stream().map(id -> new Vertex<>(id, NullValue.getInstance())).collect(Collectors.toList());
    }
}

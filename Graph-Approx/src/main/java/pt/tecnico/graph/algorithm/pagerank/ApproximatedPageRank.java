package pt.tecnico.graph.algorithm.pagerank;

import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.io.TypeSerializerInputFormat;
import org.apache.flink.api.java.io.TypeSerializerOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.graph.EdgeDirection;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;
import pt.tecnico.graph.algorithm.SimplePageRank;
import pt.tecnico.graph.algorithm.SummarizedGraphPageRank;
import pt.tecnico.graph.stream.GraphStreamHandler;
import pt.tecnico.graph.stream.GraphUpdateTracker;
import pt.tecnico.graph.stream.StreamProvider;

import java.util.Iterator;
import java.util.Set;

/**
 * Created by Renato on 26/06/2016.
 */
public class ApproximatedPageRank extends GraphStreamHandler<Tuple2<Long, Double>> {
    private ApproximatedPageRankConfig config;

    private PageRankQueryObserver observer;
    private TypeSerializerInputFormat<Tuple2<Long, Double>> rankInputFormat;
    private TypeSerializerOutputFormat<Tuple2<Long, Double>> rankOutputFormat;
    private TypeInformation<Tuple2<Long, Double>> rankTypeInfo;
    private Graph<Long, Double, Double> summaryGraph;

    public ApproximatedPageRank(StreamProvider<String> updateStream, Graph<Long, NullValue, NullValue> graph) {
        super(updateStream, graph);
    }

    @Override
    public void init() throws Exception {
        graphUpdateTracker = new GraphUpdateTracker<>(graph);

        TypeInformation<Tuple2<Long, Long>> edgeTypeInfo = graph.getEdgeIds().getType();
        edgeInputFormat = new TypeSerializerInputFormat<>(edgeTypeInfo);

        edgeOutputFormat = new TypeSerializerOutputFormat<>();
        edgeOutputFormat.setInputType(edgeTypeInfo, env.getConfig());
        edgeOutputFormat.setWriteMode(FileSystem.WriteMode.OVERWRITE);

        edgeOutputFormat.setOutputFilePath(new Path("cache/edges" + iteration));
        graph.getEdgeIds().output(edgeOutputFormat);

        env.execute("Original graph reading");

        DataSet<Tuple2<Long, Double>> ranks = computeExact();

        rankTypeInfo = ranks.getType();
        rankInputFormat = new TypeSerializerInputFormat<>(rankTypeInfo);
        rankOutputFormat = new TypeSerializerOutputFormat<>();
        rankOutputFormat.setInputType(rankTypeInfo, env.getConfig());
        rankOutputFormat.setWriteMode(FileSystem.WriteMode.OVERWRITE);
        rankOutputFormat.setOutputFilePath(new Path("cache/ranks" + iteration));

        // iteration 0
        ranks.output(rankOutputFormat);
        outputResult("", ranks);
        env.execute("First PageRank calculation");

        graphUpdateTracker.resetAll();
    }

    @Override
    public void run() {
        try {
            observer.onStart();
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }

        while (true) {
            try {
                String update = pendingUpdates.take();
                String[] split = update.split(" ");
                switch (split[0]) {
                    case "A": {
                        // add edge
                        registerEdgeAdd(split);
                        break;
                    }
                    case "D":
                        // delete
                        registerEdgeDelete(split);
                        break;
                    case "Q":
                        iteration++;

                        String tag = split.length > 1 ? split[1] : "";

                        rankInputFormat.setFilePath("cache/ranks" + ((iteration - 1) % 5));
                        DataSet<Tuple2<Long, Double>> previousRanks = env.createInput(rankInputFormat, rankTypeInfo);

                        DeciderResponse response = observer.onQuery(iteration, update, graph, graphUpdateTracker);

                        rankOutputFormat.setOutputFilePath(new Path("cache/ranks" + (iteration % 5)));

                        DataSet<Tuple2<Long, Double>> newRanks = null;

                        if (response != DeciderResponse.NO_UPDATE_AND_REPEAT_LAST_ANSWER) {
                            applyUpdates();
                        }

                        switch (response) {
                            case NO_UPDATE_AND_REPEAT_LAST_ANSWER:
                            case UPDATE_AND_REPEAT_LAST_ANSWER:
                                newRanks = previousRanks;
                                summaryGraph = null;
                                break;
                            case COMPUTE_APPROXIMATE:
                                newRanks = computeApproximate(previousRanks);
                                break;
                            case COMPUTE_EXACT:
                                newRanks = computeExact();
                                summaryGraph = null;
                                break;
                        }

                        assert newRanks != null : "Ranks are null"; //should never happen
                        outputResult(tag, newRanks);
                        newRanks.output(rankOutputFormat);

                        env.execute("Approx PageRank it. " + iteration);
                        observer.onQueryResult(iteration, update, response, graph, summaryGraph, newRanks, env.getLastJobExecutionResult());

                        break;
                    case "END":
                        observer.onStop();
                        return;
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private DataSet<Tuple2<Long, Double>> computeExact() throws Exception {
        return graph.run(new SimplePageRank<>(config.getBeta(), 1.0, config.getIterations()));
    }

    private DataSet<Tuple2<Long, Double>> computeApproximate(DataSet<Tuple2<Long, Double>> previousRanks) throws Exception {
        Set<Long> updatedIds = graphUpdateTracker.updatedAboveThresholdVertexIds(config.getUpdatedRatioThreshold(), EdgeDirection.ALL);

        Vertex<Long, Double> bigVertex = new Vertex<>(0L, 0.0);
        summaryGraph = new SummaryGraphBuilder<>(graph, 1.0)
                .representativeGraph(env.fromCollection(updatedIds, TypeInformation.of(Long.class)),
                        previousRanks, config.getNeighborhoodSize(), bigVertex);

        DataSet<Tuple2<Long, Double>> ranks = summaryGraph.run(new SummarizedGraphPageRank(config.getBeta(), config.getIterations(), bigVertex.getId()));
        ranks = previousRanks.coGroup(ranks)
                .where(0).equalTo(0)
                .with((Iterable<Tuple2<Long, Double>> previous, Iterable<Tuple2<Long, Double>> newRanks, Collector<Tuple2<Long, Double>> out) -> {
                    Iterator<Tuple2<Long, Double>> prevIt = previous.iterator();
                    Iterator<Tuple2<Long, Double>> newIt = newRanks.iterator();

                    if (newIt.hasNext()) {
                        Tuple2<Long, Double> next = newIt.next();
                        if (!next.f0.equals(bigVertex.getId())) {
                            out.collect(next);
                        }
                    } else if (prevIt.hasNext()) {
                        out.collect(prevIt.next());
                    }
                }).returns(new TypeHint<Tuple2<Long, Double>>() {
                }).withForwardedFieldsFirst("f0;f1");

        graphUpdateTracker.reset(updatedIds);
        return ranks;
    }

    private void outputResult(String date, DataSet<Tuple2<Long, Double>> ranks) {
        outputFormat.setIteration(iteration);
        outputFormat.setTags(date);
        ranks.sortPartition(1, Order.DESCENDING).setParallelism(1).first(config.getOutputSize()).output(outputFormat);
    }

    public ApproximatedPageRankConfig getConfig() {
        return config;
    }

    public void setConfig(ApproximatedPageRankConfig config) {
        this.config = config;
    }

    public ApproximatedPageRank setObserver(PageRankQueryObserver observer) {
        this.observer = observer;
        return this;
    }

    public enum DeciderResponse {
        NO_UPDATE_AND_REPEAT_LAST_ANSWER,
        UPDATE_AND_REPEAT_LAST_ANSWER,
        COMPUTE_APPROXIMATE,
        COMPUTE_EXACT
    }

}

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
import pt.tecnico.graph.algorithm.GraphUtils;
import pt.tecnico.graph.algorithm.SimplePageRank;
import pt.tecnico.graph.algorithm.SummarizedGraphPageRank;
import pt.tecnico.graph.stream.*;

import java.util.Iterator;
import java.util.Set;

public class ApproximatedPageRank extends GraphStreamHandler<Tuple2<Long, Double>> {
    private ApproximatedPageRankConfig config;

    private PageRankQueryObserver<Long, NullValue> observer;
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

                        GraphUpdates<Long, NullValue> graphUpdates = graphUpdateTracker.getGraphUpdates();
                        GraphUpdateStatistics statistics = graphUpdateTracker.getUpdateStatistics();
                        boolean result = observer.beforeUpdates(graphUpdates, statistics);

                        if (result) {
                            applyUpdates();
                            graphUpdateTracker.resetUpdates();
                        }

                        rankInputFormat.setFilePath("cache/ranks" + ((iteration - 1) % 5));
                        DataSet<Tuple2<Long, Double>> previousRanks = env.createInput(rankInputFormat, rankTypeInfo);

                        ObserverResponse response = observer.onQuery(iteration, update, graph, graphUpdates, statistics,
                                graphUpdateTracker.getUpdateInfos(), config);

                        rankOutputFormat.setOutputFilePath(new Path("cache/ranks" + (iteration % 5)));

                        DataSet<Tuple2<Long, Double>> newRanks = null;

                        switch (response) {
                            case REPEAT_LAST_ANSWER:
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

        DataSet<Tuple2<Long, Double>> result =
                graph.run(new SimplePageRank<>(config.getBeta(), 1.0, config.getIterations()));
        graphUpdateTracker.resetAll();
        return result;
    }

    private DataSet<Tuple2<Long, Double>> computeApproximate(DataSet<Tuple2<Long, Double>> previousRanks) throws Exception {
        Set<Long> updatedIds = GraphUpdateTracker.updatedAboveThresholdIds(graphUpdateTracker.getUpdateInfos(),
                config.getUpdatedRatioThreshold(), EdgeDirection.IN);

        DataSet<Long> vertices = env.fromCollection(updatedIds, TypeInformation.of(Long.class));

        // Expand the selected vertices to neighborhood given by level
        DataSet<Long> expandedVertices = GraphUtils.expandedVertexIds(graph, vertices, config.getNeighborhoodSize());

        Vertex<Long, Double> bigVertex = new Vertex<>(0L, 0.0);
        summaryGraph = new SummaryGraphBuilder<>(graph, 1.0).summaryGraph(expandedVertices, previousRanks, bigVertex);

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
                }).name("Merge with previous");

        graphUpdateTracker.reset(expandedVertices.collect());
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

    public ApproximatedPageRank setObserver(PageRankQueryObserver<Long, NullValue> observer) {
        this.observer = observer;
        return this;
    }
}

package pt.tecnico.graph.job.pagerank;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.io.TypeSerializerInputFormat;
import org.apache.flink.api.java.io.TypeSerializerOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.EdgeDirection;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;
import pt.tecnico.graph.GraphUtils;
import pt.tecnico.graph.algorithm.ApproximatedSimplePageRank;
import pt.tecnico.graph.algorithm.SimplePageRank;
import pt.tecnico.graph.job.DegreeTracker;
import pt.tecnico.graph.job.StreamHandler;
import pt.tecnico.graph.stream.StreamProvider;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by Renato on 26/06/2016.
 */
public class ApproximatedPageRank extends StreamHandler {
    private final DegreeTracker<Long> degreeTracker;
    private ApproximatedPageRankConfig config;
    private Set<Edge<Long, NullValue>> edgesToAdd = new HashSet<>();
    private Set<Edge<Long, NullValue>> edgesToRemove = new HashSet<>();

    private TypeSerializerInputFormat<Tuple2<Long, Long>> edgeInputFormat;
    private TypeSerializerOutputFormat<Tuple2<Long, Long>> edgeOutputFormat;

    private TypeSerializerInputFormat<Tuple2<Long, Double>> rankInputFormat;
    private TypeSerializerOutputFormat<Tuple2<Long, Double>> rankOutputFormat;

    private DataSet<Long> updated = GraphUtils.emptyDataSet(env, Long.class);
    private int iteration = 0;
    private String csvName = null;

    public ApproximatedPageRank(StreamProvider<String> updateStream, Graph<Long, NullValue, NullValue> graph) {
        super(updateStream, graph);
        this.degreeTracker = new DegreeTracker<>(graph);
    }


    @Override
    public void run() {
        TypeInformation<Tuple2<Long, Long>> edgeTypeInfo = graph.getEdgeIds().getType();
        TypeInformation<Tuple2<Long, Double>> rankTypeInfo;
        edgeInputFormat = new TypeSerializerInputFormat<>(edgeTypeInfo);

        edgeOutputFormat = new TypeSerializerOutputFormat<>();
        edgeOutputFormat.setInputType(edgeTypeInfo, env.getConfig());
        edgeOutputFormat.setWriteMode(FileSystem.WriteMode.OVERWRITE);

        try {
            edgeOutputFormat.setOutputFilePath(new Path("./edges" + iteration));
            graph.getEdgeIds().output(edgeOutputFormat);

            DataSet<Tuple2<Long, Double>> ranks = graph.run(new SimplePageRank<>(config.getBeta(), 1.0, config.getIterations()));
            rankTypeInfo = ranks.getType();
            rankInputFormat = new TypeSerializerInputFormat<>(rankTypeInfo);

            rankOutputFormat = new TypeSerializerOutputFormat<>();
            rankOutputFormat.setInputType(rankTypeInfo, env.getConfig());
            rankOutputFormat.setWriteMode(FileSystem.WriteMode.OVERWRITE);
            rankOutputFormat.setOutputFilePath(new Path("./ranks" + iteration));

            // iteration 0
            ranks.output(rankOutputFormat);
            csvName = String.format("pageRank%04d.csv", iteration);
            outputResult(csvName, ranks);

            env.execute("First PageRank calculation");
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
                        applyUpdates();
                        String date = split[1];

                        DataSet<Tuple2<Long, Double>> truePR = graph.run(new SimplePageRank<>(config.getBeta(), 1.0, config.getIterations()));
                        outputResult(String.format("true_pageRank%04d_%s.csv", iteration, date), truePR);
                        env.execute("True PageRank it. " + iteration);

                        rankInputFormat.setFilePath("ranks" + ((iteration - 1) % 5));
                        DataSet<Tuple2<Long, Double>> previousRanks = env.createInput(rankInputFormat, rankTypeInfo);

                        updated = degreeTracker.allUpdatedVertexIds(EdgeDirection.ALL);
                        rankOutputFormat.setOutputFilePath(new Path("./ranks" + (iteration % 5)));

                        if (updated.count() == 0) {
                            rankOutputFormat.setOutputFilePath(new Path("./ranks" + (iteration % 5)));
                            previousRanks.output(rankOutputFormat);

                            csvName = String.format("pageRank%04d_%s.csv", iteration, date);
                            outputResult(csvName, previousRanks);
                            env.execute("Approx PageRank it. " + iteration);
                            continue;
                        }

                        Vertex<Long, Double> bigVertex = new Vertex<>(0L, 0.0);
                        Graph<Long, Double, Double> representativeGraph = new RepresentativeGraphBuilder<>(graph, 1.0).representativeGraph(updated, previousRanks, config.getNeighborhoodSize(), bigVertex);

                        System.err.println("Iteration " + iteration);
                        System.err.println("total vertices: " + graph.numberOfVertices());
                        System.err.println("total edges: " + graph.numberOfEdges());
                        System.err.println("repr. vertices: " + representativeGraph.numberOfVertices());
                        System.err.println("repr. edges: " + representativeGraph.numberOfEdges());

                        DataSet<Tuple2<Long, Double>> ranks = representativeGraph.run(new ApproximatedSimplePageRank(config.getBeta(), config.getIterations(), bigVertex.getId()));

                        DataSet<Tuple2<Long, Double>> newRanks = previousRanks.coGroup(ranks)
                                .where(0).equalTo(0)
                                .with(new CoGroupFunction<Tuple2<Long, Double>, Tuple2<Long, Double>, Tuple2<Long, Double>>() {
                                    @Override
                                    public void coGroup(Iterable<Tuple2<Long, Double>> previous, Iterable<Tuple2<Long, Double>> newRanks, Collector<Tuple2<Long, Double>> out) throws Exception {
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
                                    }
                                });

                        rankOutputFormat.setOutputFilePath(new Path("./ranks" + (iteration % 5)));
                        newRanks.output(rankOutputFormat);

                        csvName = String.format("pageRank%04d_%s.csv", iteration, date);
                        outputResult(csvName, newRanks);
                        env.execute("Approx PageRank it. " + iteration);

                        degreeTracker.reset(updated);
                        break;
                    case "END":
                        return;
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private void outputResult(String path, DataSet<Tuple2<Long, Double>> ranks) {
        ranks = ranks.sortPartition(1, Order.DESCENDING).setParallelism(1).first(config.getOutputSize());
        if (config.isPrintRanks()) {
            ranks.writeAsCsv(path, FileSystem.WriteMode.OVERWRITE);
        } else {
            ranks.project(0).writeAsCsv(path, FileSystem.WriteMode.OVERWRITE);
        }
    }

    private void applyUpdates() throws Exception {
        edgeInputFormat.setFilePath("./edges" + ((iteration - 1) % 5));
        graph = Graph.fromTuple2DataSet(env.createInput(edgeInputFormat), env);

        if (!edgesToAdd.isEmpty()) {
            List<Vertex<Long, NullValue>> vertices = edgesToAdd.stream()
                    .flatMap(e -> Stream.of(e.getSource(), e.getTarget()))
                    .map(id -> new Vertex<>(id, NullValue.getInstance()))
                    .distinct()
                    .collect(Collectors.toList());

            graph = graph
                    .addVertices(vertices)
                    .addEdges(new ArrayList<>(edgesToAdd));
        }

        if (!edgesToRemove.isEmpty()) {
            graph = graph.removeEdges(new ArrayList<>(edgesToRemove));
        }

        edgeOutputFormat.setOutputFilePath(new Path("./edges" + (iteration % 5)));
        graph.getEdgeIds().output(edgeOutputFormat);
        env.execute("Apply updates it. " + iteration);

        edgesToAdd.clear();
        edgesToRemove.clear();
    }

    private void registerEdgeDelete(String[] split) {
        Edge<Long, NullValue> edge = parseEdge(split);
        edgesToRemove.add(edge);
        degreeTracker.removeEdge(edge.getSource(), edge.getTarget());
    }

    private void registerEdgeAdd(String[] split) {
        Vertex<Long, NullValue>[] vertices = parseVertices(split);
        Edge<Long, NullValue> edge = parseEdge(split);
        edgesToAdd.add(edge);
        degreeTracker.addEdge(vertices[0].getId(), vertices[1].getId());
    }

    @SuppressWarnings("unchecked")
    private Vertex<Long, NullValue>[] parseVertices(String[] data) {
        assert data.length == 3;
        Vertex<Long, NullValue> v1 = new Vertex<>(Long.valueOf(data[1]), NullValue.getInstance());
        Vertex<Long, NullValue> v2 = new Vertex<>(Long.valueOf(data[2]), NullValue.getInstance());
        return (Vertex<Long, NullValue>[]) new Vertex[]{v1, v2};
    }

    private Edge<Long, NullValue> parseEdge(String[] data) {
        assert data.length == 3;
        return new Edge<>(Long.valueOf(data[1]), Long.valueOf(data[2]), NullValue.getInstance());
    }

    public ApproximatedPageRankConfig getConfig() {
        return config;
    }

    public ApproximatedPageRank setConfig(ApproximatedPageRankConfig config) {
        this.config = config;
        return this;
    }
}

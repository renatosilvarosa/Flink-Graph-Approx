package pt.tecnico.graph.algorithm.topdegree;

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
import org.apache.flink.types.LongValue;
import org.apache.flink.types.NullValue;
import pt.tecnico.graph.stream.GraphStreamHandler;
import pt.tecnico.graph.stream.GraphUpdateTracker;
import pt.tecnico.graph.stream.StreamProvider;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by Renato on 24/06/2016.
 */
public class TopDegree extends GraphStreamHandler<Tuple2<Long, LongValue>> {

    private final EdgeDirection edgeDirection;

    private GraphUpdateTracker<Long> graphUpdateTracker;

    private Set<Edge<Long, NullValue>> edgesToAdd = new HashSet<>();
    private Set<Edge<Long, NullValue>> edgesToRemove = new HashSet<>();

    private TypeSerializerInputFormat<Tuple2<Long, Long>> binInputFormat;
    private TypeSerializerOutputFormat<Tuple2<Long, Long>> binOutputFormat;
    private DataSet<Tuple2<Long, LongValue>> topVertices = null;
    private String csvName = null;
    private Set<Long> updated = new HashSet<>();
    private int iteration = 0;

    public TopDegree(StreamProvider<String> updateStream, Graph<Long, NullValue, NullValue> graph, EdgeDirection edgeDirection) {
        super(updateStream, graph);
        this.graphUpdateTracker = new GraphUpdateTracker<>(graph);
        this.edgeDirection = edgeDirection;
    }

    @Override
    public void run() {
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
                        applyUpdates();

                        String date = split[1];
                        double th = Double.parseDouble(split[2]);

                        updated = graphUpdateTracker.allUpdatedVertexIds(edgeDirection);
                        topVertices = queryTop(th);

                        csvName = "./top_" + date + "_" + th + ".csv";
                        topVertices.writeAsCsv(csvName, FileSystem.WriteMode.OVERWRITE);
                        topVertices.output(outputFormat);
                        env.execute();

                        graphUpdateTracker.reset(updated);
                        break;
                    case "END":
                        return;
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private DataSet<Tuple2<Long, LongValue>> queryTop(double th) throws Exception {
        if (topVertices == null) {
            return topDegree(graph, th);
        }

        DataSet<Tuple2<Long, LongValue>> updatedVertices = graph.inDegrees()
                .join(env.fromCollection(updated, TypeInformation.of(Long.class)))
                .where(0).equalTo(v -> v)
                .with((degree, id) -> degree)
                .returns(graph.inDegrees().getType());

        Double number = graphUpdateTracker.numberOfVertices() * th + 1;

        return env.readCsvFile(csvName).types(Long.class, LongValue.class)
                .union(updatedVertices)
                .distinct(0)
                .sortPartition(1, Order.DESCENDING)
                .first(number.intValue());
    }

    private void applyUpdates() {
        binInputFormat.setFilePath("./edges" + (iteration % 5));
        graph = Graph.fromTuple2DataSet(env.createInput(binInputFormat), env);

        if (!edgesToAdd.isEmpty()) {
            List<Vertex<Long, NullValue>> vertices = edgesToAdd.stream()
                    .flatMap(e -> Stream.of(e.getSource(), e.getTarget()))
                    .map(id -> new Vertex<>(id, NullValue.getInstance()))
                    .distinct()
                    .collect(Collectors.toList());

            graph = graph
                    .addVertices(vertices)
                    .addEdges(new ArrayList<>(edgesToAdd));

            edgesToAdd.clear();
        }

        if (!edgesToRemove.isEmpty()) {
            graph = graph.removeEdges(new ArrayList<>(edgesToRemove));
            edgesToRemove.clear();
        }

        iteration++;
        binOutputFormat.setOutputFilePath(new Path("./edges" + (iteration % 5)));
        graph.getEdgeIds().output(binOutputFormat);
    }

    private void registerEdgeDelete(String[] split) {
        Edge<Long, NullValue> edge = parseEdge(split);
        edgesToRemove.add(edge);
        graphUpdateTracker.removeEdge(edge.getSource(), edge.getTarget());
    }

    private void registerEdgeAdd(String[] split) {
        Vertex<Long, NullValue>[] vertices = parseVertices(split);
        Edge<Long, NullValue> edge = parseEdge(split);
        edgesToAdd.add(edge);
        graphUpdateTracker.addEdge(vertices[0].getId(), vertices[1].getId());
    }

    private DataSet<Tuple2<Long, LongValue>> topDegree(Graph<Long, NullValue, NullValue> graph, double ratio) throws Exception {
        Double number = graph.numberOfVertices() * ratio + 1;
        return graph.inDegrees().sortPartition(1, Order.DESCENDING).first(number.intValue());
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

    @Override
    public void init() throws Exception {
        TypeInformation<Tuple2<Long, Long>> edgeTypeInfo = graph.getEdgeIds().getType();
        binInputFormat = new TypeSerializerInputFormat<>(edgeTypeInfo);

        binOutputFormat = new TypeSerializerOutputFormat<>();
        binOutputFormat.setInputType(edgeTypeInfo, env.getConfig());
        binOutputFormat.setWriteMode(FileSystem.WriteMode.OVERWRITE);

        binOutputFormat.setOutputFilePath(new Path("./edges" + iteration));
        graph.getEdgeIds().output(binOutputFormat);
        env.execute();
    }
}
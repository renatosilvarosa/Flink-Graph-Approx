package pt.tecnico.graph.job;

import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
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
import pt.tecnico.graph.GraphUtils;
import pt.tecnico.graph.stream.StreamProvider;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by Renato on 25/05/2016.
 */
public class StreamHandler {
    protected Graph<Long, NullValue, NullValue> graph;
    private StreamProvider<String> updateStream;
    private BlockingQueue<String> pendingUpdates;
    private int iteration = 0;
    private DegreeTracker<Long> degreeTracker;
    private ExecutionEnvironment env;

    public void start() {
        new Thread(updateStream, "Stream thread").start();
        new Thread(new TopCitedPapers(), "Update Graph Thread").start();
    }

    public void setGraph(Graph<Long, NullValue, NullValue> graph) {
        this.graph = graph;
        this.degreeTracker = new DegreeTracker<>(graph);
        this.env = graph.getContext();
    }

    public void setUpdateStream(StreamProvider<String> stream) {
        this.updateStream = stream;
        this.pendingUpdates = stream.getQueue();
    }

    private DataSet<Tuple2<Long, Long>> topDegree(Graph<Long, NullValue, NullValue> graph, double ratio) throws Exception {
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

    private class TopCitedPapers implements Runnable {
        Set<Edge<Long, NullValue>> edgesToAdd = new HashSet<>();
        Set<Edge<Long, NullValue>> edgesToRemove = new HashSet<>();

        TypeInformation<Tuple2<Long, Long>> typeInfo = graph.getEdgeIds().getType();

        TypeSerializerInputFormat<Tuple2<Long, Long>> binInputFormat = new TypeSerializerInputFormat<>(typeInfo);
        TypeSerializerOutputFormat<Tuple2<Long, Long>> binOutputFormat = new TypeSerializerOutputFormat<>();
        DataSet<Tuple2<Long, Long>> topVertices = null;
        String csvName = null;
        DataSet<Long> updated = GraphUtils.emptyDataSet(env, Long.class);


        @Override
        public void run() {
            binOutputFormat.setInputType(typeInfo, env.getConfig());
            binOutputFormat.setWriteMode(FileSystem.WriteMode.OVERWRITE);

            try {
                binOutputFormat.setOutputFilePath(new Path("./edges" + iteration));
                //graph = Graph.fromTuple2DataSet(env.createInput(binInputFormat), env);
                graph.getEdgeIds().output(binOutputFormat);
                env.execute();
            } catch (Exception e) {
                e.printStackTrace();
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
                            applyUpdates();

                            String date = split[1];
                            double th = Double.parseDouble(split[2]);
                            //DataSet<Long> ids = degreeTracker.updatedAboveThresholdVertexIds(th, EdgeDirection.ALL);
                            //degreeTracker.reset(ids);

                            updated = degreeTracker.allUpdatedVertexIds(EdgeDirection.IN);
                            topVertices = queryTop(th);

                            csvName = "./top_" + date + ".csv";
                            topVertices.writeAsCsv(csvName, FileSystem.WriteMode.OVERWRITE);
                            env.execute();

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

        private DataSet<Tuple2<Long, Long>> queryTop(double th) throws Exception {
            if (topVertices == null) {
                return topDegree(graph, th);
            }

            DataSet<Tuple2<Long, Long>> updatedVertices = graph.inDegrees()
                    .join(updated)
                    .where(0).equalTo(v -> v)
                    .map(v -> v.f0)
                    .returns(graph.inDegrees().getType());

            Double number = degreeTracker.numberOfVertices() * th + 1;

            return env.readCsvFile(csvName).types(Long.class, Long.class)
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
            degreeTracker.removeEdge(edge.getSource(), edge.getTarget());
        }

        private void registerEdgeAdd(String[] split) {
            Vertex<Long, NullValue>[] vertices = parseVertices(split);
            Edge<Long, NullValue> edge = parseEdge(split);
            edgesToAdd.add(edge);
            degreeTracker.addEdge(vertices[0].getId(), vertices[1].getId());
        }
    }
}

package pt.tecnico.graph.algorithm.topdegree;

import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.io.TypeSerializerInputFormat;
import org.apache.flink.api.java.io.TypeSerializerOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.graph.EdgeDirection;
import org.apache.flink.graph.Graph;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.NullValue;
import pt.tecnico.graph.stream.GraphStreamHandler;
import pt.tecnico.graph.stream.GraphUpdateTracker;
import pt.tecnico.graph.stream.StreamProvider;

import java.util.HashSet;
import java.util.Set;

public class TopDegree extends GraphStreamHandler<Tuple2<Long, LongValue>> {

    private final EdgeDirection edgeDirection;

    private GraphUpdateTracker<Long, NullValue, NullValue> graphUpdateTracker;
    private DataSet<Tuple2<Long, LongValue>> topVertices = null;
    private String csvName = null;
    private Set<Long> updated = new HashSet<>();

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

                        updated = GraphUpdateTracker.allUpdatedIds(graphUpdateTracker.getUpdateInfos(), edgeDirection);
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

        Double number = graphUpdateTracker.getUpdateStatistics().getTotalVertices() * th + 1;

        return env.readCsvFile(csvName).types(Long.class, LongValue.class)
                .union(updatedVertices)
                .distinct(0)
                .sortPartition(1, Order.DESCENDING)
                .first(number.intValue());
    }

    private DataSet<Tuple2<Long, LongValue>> topDegree(Graph<Long, NullValue, NullValue> graph, double ratio) throws Exception {
        Double number = graph.numberOfVertices() * ratio + 1;
        return graph.inDegrees().sortPartition(1, Order.DESCENDING).first(number.intValue());
    }

    @Override
    public void init() throws Exception {
        TypeInformation<Tuple2<Long, Long>> edgeTypeInfo = graph.getEdgeIds().getType();
        edgeInputFormat = new TypeSerializerInputFormat<>(edgeTypeInfo);

        edgeOutputFormat = new TypeSerializerOutputFormat<>();
        edgeOutputFormat.setInputType(edgeTypeInfo, env.getConfig());
        edgeOutputFormat.setWriteMode(FileSystem.WriteMode.OVERWRITE);

        edgeOutputFormat.setOutputFilePath(new Path("./edges" + iteration));
        graph.getEdgeIds().output(edgeOutputFormat);
        env.execute();
    }
}
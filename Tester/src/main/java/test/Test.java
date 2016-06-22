package test;

import org.apache.flink.api.common.io.FileOutputFormat;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.SampleWithFraction;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import pt.tecnico.graph.GraphUtils;
import pt.tecnico.graph.algorithm.SimplePageRank;
import pt.tecnico.graph.job.EdgeStreamTimedApproxJob;
import pt.tecnico.graph.job.GraphJobConfiguration;
import pt.tecnico.graph.output.RollingCsvOutputFormat;
import pt.tecnico.graph.stream.SocketStreamProvider;

import java.util.Random;

/**
 * Created by Renato on 30/03/2016.
 */
public class Test {
    public static void main(String[] args) {
        ExecutionEnvironment env = ExecutionEnvironment.createRemoteEnvironment("localhost", 6123,
                "flink-graph-algorithms-0.1.jar", "flink-graph-approx-0.1.jar");
        //ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
/*
        DataSource<Tuple2<Integer, Integer>> edges = env.fromElements(Tuple2.of(1, 2), Tuple2.of(1, 3), Tuple2.of(2, 3));
        Graph<Integer, NullValue, NullValue> initialGraph = Graph.fromTuple2DataSet(edges, env);

        List<Edge<Integer, NullValue>> edgeUpdates = Arrays.asList(Tuple2.of(2, 4), Tuple2.of(2, 5), Tuple2.of(3, 4), Tuple2.of(3, 5), Tuple2.of(4, 5),
                Tuple2.of(4, 6), Tuple2.of(5, 6), Tuple2.of(1, 6)).stream().map(t -> new Edge<>(t.f0, t.f1, NullValue.getInstance())).collect(Collectors.toList());

        EdgeStreamTimedApproxJob<Integer> graphJob = new EdgeStreamTimedApproxJob<>(new GraphJobConfiguration(1000, 3));
        graphJob.setGraph(initialGraph);
        graphJob.setUpdateStream(new CollectionStreamProvider<>(edgeUpdates));

        graphJob.start();
        */
        Graph<Integer, NullValue, NullValue> initialGraph = GraphUtils.emptyGraph(env, Integer.class);
        //Graph<Integer, NullValue, NullValue> initialGraph = Graph.fromCsvReader(args[0], env).fieldDelimiterEdges("\t").keyType(Integer.class);

        EdgeStreamTimedApproxJob<Integer, NullValue, Vertex<Integer, Double>> graphJob = new EdgeStreamTimedApproxJob<>(new GraphJobConfiguration(2000, 50));
        //EdgeAddDeleteStreamJob<Integer, Double> graphJob = new EdgeAddDeleteStreamJob<>(new GraphJobConfiguration(10000, 2000));
        graphJob.setGraph(initialGraph);

        DataSet<Vertex<Integer, NullValue>> vertexSample = initialGraph.getVertices().mapPartition(new SampleWithFraction<>(false, 0.1, new Random().nextLong()));
        DataSet<Edge<Integer, NullValue>> edges = initialGraph.getEdges()
                .join(vertexSample).where(0).equalTo(0)
                .with((edge, vertex) -> edge).returns(new TypeHint<Edge<Integer, NullValue>>() {
                });

        Graph<Integer, NullValue, NullValue> sampledGraph = Graph.fromDataSet(vertexSample, edges, env);

/*        graphJob.setUpdateStream(new SocketStreamProvider<>("localhost", 1234, s -> {
            String[] vs = s.split("\t");
            Integer v1 = Integer.valueOf(vs[0]);
            Integer v2 = Integer.valueOf(vs[1]);
            return new Edge<>(v1, v2, NullValue.getInstance());
        }));*/

        graphJob.setUpdateStream(new SocketStreamProvider<>("localhost", 1234, s -> {
            String[] vs = s.split(",");
            Integer v1 = Integer.valueOf(vs[0]);
            Integer v2 = Integer.valueOf(vs[1]);
            return new Edge<>(v1, v2, NullValue.getInstance());
        }));

/*        graphJob.setUpdateStream(new FileStreamProvider<>(args[0], s -> {
            String[] vs = s.split("\t");
            Integer v1 = Integer.valueOf(vs[0]);
            Integer v2 = Integer.valueOf(vs[1]);
            return new Edge<>(v1, v2, NullValue.getInstance());
        }));*/

/*        graphJob.setUpdateStream(new FileStreamProvider<>(args[1], s -> {
            String[] vs = s.split(" ");
            Integer v1 = Integer.valueOf(vs[1]);
            Integer v2 = Integer.valueOf(vs[2]);
            return Tuple2.of(vs[0], new Edge<>(v1, v2, NullValue.getInstance()));
        }));*/

        graphJob.setAlgorithm(new SimplePageRank<>(0.85, 1.0, 20));
        FileOutputFormat<Vertex<Integer, Double>> outputFormat = new RollingCsvOutputFormat<>("D:\\ranks");
        outputFormat.setWriteMode(FileSystem.WriteMode.OVERWRITE);
        graphJob.setOutputFormat(outputFormat);

        graphJob.start();

    }
}

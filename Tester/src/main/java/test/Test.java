package test;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.PrintingOutputFormat;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.types.NullValue;
import pt.tecnico.graph.algorithm.AverageDegree;
import pt.tecnico.graph.ApproxGraphJob;
import pt.tecnico.graph.Configuration;
import pt.tecnico.graph.stream.SocketStreamProvider;

/**
 * Created by Renato on 30/03/2016.
 */
public class Test {
    public static void main(String[] args) {
        ExecutionEnvironment env = ExecutionEnvironment.createRemoteEnvironment("localhost", 6123, "graph-algorithms-0.1.jar");
        //ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
/*
        DataSource<Tuple2<Integer, Integer>> edges = env.fromElements(Tuple2.of(1, 2), Tuple2.of(1, 3), Tuple2.of(2, 3));
        Graph<Integer, NullValue, NullValue> initialGraph = Graph.fromTuple2DataSet(edges, env);

        List<Edge<Integer, NullValue>> edgeUpdates = Arrays.asList(Tuple2.of(2, 4), Tuple2.of(2, 5), Tuple2.of(3, 4), Tuple2.of(3, 5), Tuple2.of(4, 5),
                Tuple2.of(4, 6), Tuple2.of(5, 6), Tuple2.of(1, 6)).stream().map(t -> new Edge<>(t.f0, t.f1, NullValue.getInstance())).collect(Collectors.toList());

        ApproxGraphJob<Integer> graphJob = new ApproxGraphJob<>(new Configuration(1000, 3));
        graphJob.setGraph(initialGraph);
        graphJob.setEdgeStream(new CollectionStreamProvider<>(edgeUpdates));

        graphJob.start();
        */

        DataSource<Tuple2<Integer, Integer>> edges = env.fromElements(Tuple2.of(1001, 9304045));
        Graph<Integer, NullValue, NullValue> initialGraph = Graph.fromTuple2DataSet(edges, env);

        ApproxGraphJob<Integer, NullValue, Double> graphJob = new ApproxGraphJob<>(new Configuration(1000, 50));
        graphJob.setGraph(initialGraph);

        graphJob.setEdgeStream(new SocketStreamProvider<>("localhost", 1234, s -> {
            String[] vs = s.split("\t");
            Integer v1 = Integer.valueOf(vs[0]);
            Integer v2 = Integer.valueOf(vs[1]);
            return new Edge<>(v1, v2, NullValue.getInstance());
        }));

/*        graphJob.setEdgeStream(new FileStreamProvider<>(args[0], s -> {
            String[] vs = s.split("\t");
            Integer v1 = Integer.valueOf(vs[0]);
            Integer v2 = Integer.valueOf(vs[1]);
            return new Edge<>(v1, v2, NullValue.getInstance());
        }));*/

        graphJob.setAlgorithm(new AverageDegree<>());
        graphJob.setOutputFormat(new PrintingOutputFormat<>(true));

        graphJob.start();

    }
}

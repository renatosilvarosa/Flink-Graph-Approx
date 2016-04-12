package test;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Graph;
import org.apache.flink.types.NullValue;
import pt.tecnico.graph.algorithm.SimplePageRank;

/**
 * Created by Renato on 09/04/2016.
 */
public class Main {
    public static void main(String[] args) {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        try {
            Graph<Integer, NullValue, NullValue> graph = Graph.fromCsvReader("D:\\Documents\\Dissertação\\Datasets\\simple.txt", env).fieldDelimiterEdges(" ").keyType(Integer.class);
            graph.run(new SimplePageRank<>(0.85, 1.0, 30)).print();

            //Graph<Integer, NullValue, NullValue> graph = GraphUtils.emptyGraph(env, Integer.class);
            //System.out.println(graph.numberOfVertices());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

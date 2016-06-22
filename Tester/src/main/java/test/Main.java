package test;

import org.apache.flink.api.java.ExecutionEnvironment;
import pt.tecnico.graph.GraphUtils;
import pt.tecnico.graph.job.StreamHandler;
import pt.tecnico.graph.stream.SocketStreamProvider;

/**
 * Created by Renato on 09/04/2016.
 */
public class Main {
    public static void main(String[] args) {
        //ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        ExecutionEnvironment env = ExecutionEnvironment.createRemoteEnvironment("localhost", 6123,
                "flink-graph-approx-0.1.jar"
        );
        try {
            StreamHandler streamHandler = new StreamHandler();
            streamHandler.setUpdateStream(new SocketStreamProvider<>("localhost", 1234, s -> s));
            streamHandler.setGraph(GraphUtils.emptyGraph(env, Long.class));
            streamHandler.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

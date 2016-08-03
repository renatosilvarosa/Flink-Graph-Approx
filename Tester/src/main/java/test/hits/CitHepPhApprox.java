package test.hits;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Graph;
import org.apache.flink.types.NullValue;
import pt.tecnico.graph.algorithm.hits.ApproximatedHITS;
import pt.tecnico.graph.algorithm.hits.ApproximatedHITSConfig;
import pt.tecnico.graph.stream.SocketStreamProvider;

/**
 * Created by Renato on 09/04/2016.
 */
public class CitHepPhApprox {
    public static void main(String[] args) {
        String localDir = args[0];
        int iterations = Integer.parseInt(args[1]);
        int outputSize = Integer.parseInt(args[2]);

        ExecutionEnvironment env = ExecutionEnvironment.createRemoteEnvironment("146.193.41.145", 6123,
                "flink-graph-approx-0.1.jar", "flink-graph-algorithms-0.1.jar"
        );

        env.getConfig().disableSysoutLogging().setParallelism(1);

        try {
            String edgesPath = "/home/rrosa/Datasets/Cit-HepPh/Cit-HepPh-init.txt";
            Graph<Long, NullValue, NullValue> graph = Graph.fromCsvReader(edgesPath, env)
                    .ignoreCommentsEdges("#")
                    .fieldDelimiterEdges("\t")
                    .keyType(Long.class);

            ApproximatedHITSConfig config = new ApproximatedHITSConfig()
                    .setIterations(iterations)
                    .setOutputSize(outputSize);

            ApproximatedHITS approximatedHITS = new ApproximatedHITS(new SocketStreamProvider("localhost", 1234), graph);

            approximatedHITS.setConfig(config);

            String dir = localDir + "/Statistics/CitHepPh";
            approximatedHITS.setObserver(new ApproximatedHITSStatistics(dir, args[3]));

            approximatedHITS.start();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

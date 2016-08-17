package test.hits.polblogs;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Graph;
import org.apache.flink.types.NullValue;
import pt.tecnico.graph.algorithm.hits.ApproximatedHITS;
import pt.tecnico.graph.algorithm.hits.ApproximatedHITSConfig;
import pt.tecnico.graph.algorithm.hits.HITSCsvOutputFormat;
import pt.tecnico.graph.stream.SocketStreamProvider;
import test.hits.ExactHITSStatistics;

/**
 * Created by Renato on 09/04/2016.
 */
public class HITSPolBlogsExact {
    public static void main(String[] args) {
        String localDir = args[0];
        String remoteDir = args[1];
        int iterations = Integer.parseInt(args[2]);
        int outputSize = Integer.parseInt(args[3]);

        ExecutionEnvironment env = ExecutionEnvironment.createRemoteEnvironment("146.193.41.145", 6123,
                "flink-graph-approx-0.3.jar", "flink-graph-algorithms-0.3.jar"
        );

        env.getConfig()
                .disableSysoutLogging()
                .setParallelism(1);
        try {
            Graph<Long, NullValue, NullValue> graph = Graph.fromCsvReader(remoteDir + "/Datasets/polblogs/polblogs_init.csv", env)
                    .ignoreCommentsEdges("#")
                    .fieldDelimiterEdges(";")
                    .keyType(Long.class);

            ApproximatedHITSConfig config = new ApproximatedHITSConfig()
                    .setIterations(iterations)
                    .setOutputSize(outputSize);

            ApproximatedHITS approximatedHITS = new ApproximatedHITS(new SocketStreamProvider("localhost", 1234), graph);

            HITSCsvOutputFormat hubOutputFormat = new HITSCsvOutputFormat(remoteDir + "/Results/HITS/polblogs-exact", System.lineSeparator(), ";", false, true);
            hubOutputFormat.setName("exact_hits");
            approximatedHITS.setHubOutputFormat(hubOutputFormat);

            HITSCsvOutputFormat authOutputFormat = new HITSCsvOutputFormat(remoteDir + "/Results/HITS/polblogs-exact", System.lineSeparator(), ";", false, true);
            authOutputFormat.setName("exact_hits");
            approximatedHITS.setAuthorityOutputFormat(authOutputFormat);

            approximatedHITS.setConfig(config);

            String dir = localDir + "/Statistics/HITS/polblogs";
            approximatedHITS.setObserver(new ExactHITSStatistics(dir, args[4]));

            approximatedHITS.start();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}

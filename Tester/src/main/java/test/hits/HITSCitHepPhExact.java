package test.hits;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Graph;
import org.apache.flink.types.NullValue;
import pt.tecnico.graph.algorithm.hits.ApproximatedHITS;
import pt.tecnico.graph.algorithm.hits.ApproximatedHITSConfig;
import pt.tecnico.graph.algorithm.hits.HITSCsvOutputFormat;
import pt.tecnico.graph.stream.SocketStreamProvider;

/**
 * Created by Renato on 09/04/2016.
 */
public class HITSCitHepPhExact {
    public static void main(String[] args) {
        String localDir = args[0];
        String remoteDir = args[1];
        int iterations = Integer.parseInt(args[2]);
        int outputSize = Integer.parseInt(args[3]);

        ExecutionEnvironment env = ExecutionEnvironment.createRemoteEnvironment("146.193.41.145", 6123,
                "flink-graph-approx-0.2.jar", "flink-graph-algorithms-0.2.jar"
        );

        env.getConfig().disableSysoutLogging().setParallelism(1);

        try {
            Graph<Long, NullValue, NullValue> graph = Graph.fromCsvReader("/home/rrosa/Datasets/Cit-HepPh/Cit-HepPh-init.txt", env)
                    .ignoreCommentsEdges("#")
                    .fieldDelimiterEdges("\t")
                    .keyType(Long.class);

            ApproximatedHITSConfig config = new ApproximatedHITSConfig()
                    .setIterations(iterations)
                    .setOutputSize(outputSize);

            ApproximatedHITS approximatedHITS = new ApproximatedHITS(new SocketStreamProvider("localhost", 1234), graph);
            HITSCsvOutputFormat outputFormat = new HITSCsvOutputFormat(remoteDir + "/Results/CitHepPh/HITS/", System.lineSeparator(), ";", false, true);
            outputFormat.setName("exact_hits");
            approximatedHITS.setOutputFormat(outputFormat);

            approximatedHITS.setConfig(config);

            String dir = localDir + "/Statistics/CitHepPh/HITS";
            approximatedHITS.setObserver(new ExactHITSStatistics(dir, args[4]));

            approximatedHITS.start();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

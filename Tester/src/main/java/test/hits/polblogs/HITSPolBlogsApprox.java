package test.hits.polblogs;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Graph;
import org.apache.flink.types.NullValue;
import pt.tecnico.graph.algorithm.hits.ApproximatedHITS;
import pt.tecnico.graph.algorithm.hits.ApproximatedHITSConfig;
import pt.tecnico.graph.algorithm.hits.HITSCsvOutputFormat;
import pt.tecnico.graph.stream.FileStreamProvider;
import test.hits.ApproximatedHITSStatistics;

/**
 * Created by Renato on 09/04/2016.
 */
public class HITSPolBlogsApprox {
    public static void main(String[] args) {
        String localDir = args[0];
        String remoteDir = args[1];
        int iterations = Integer.parseInt(args[2]);
        double threshold = Double.parseDouble(args[3]);
        int neighborhoodSize = Integer.parseInt(args[4]);
        int outputSize = Integer.parseInt(args[5]);

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
                    .setUpdatedRatioThreshold(threshold)
                    .setNeighborhoodSize(neighborhoodSize)
                    .setOutputSize(outputSize);

            FileStreamProvider<String> streamProvider = new FileStreamProvider<>(localDir + "/Datasets/polblogs/polblogs_cont.csv", s -> {
                Thread.sleep(10);
                return s;
            });

            ApproximatedHITS approximatedHITS = new ApproximatedHITS(streamProvider, graph);

            approximatedHITS.setConfig(config);

            String outputDir = String.format("%s/Results/HITS/PolBlogs-%02.2f-%d", remoteDir, threshold, neighborhoodSize);
            HITSCsvOutputFormat hubOutputFormat = new HITSCsvOutputFormat(outputDir, System.lineSeparator(), ";", false, true);
            hubOutputFormat.setName("approx_hits");
            approximatedHITS.setHubOutputFormat(hubOutputFormat);

            HITSCsvOutputFormat authOutputFormat = new HITSCsvOutputFormat(outputDir, System.lineSeparator(), ";", false, true);
            authOutputFormat.setName("approx_hits");
            approximatedHITS.setAuthorityOutputFormat(authOutputFormat);

            String dir = localDir + "/Statistics/HITS/PolBlogs";
            approximatedHITS.setObserver(new ApproximatedHITSStatistics(dir, args[6]));

            approximatedHITS.start();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}

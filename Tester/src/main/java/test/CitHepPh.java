package test;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Graph;
import org.apache.flink.types.NullValue;
import pt.tecnico.graph.algorithm.pagerank.ApproximatedPageRank;
import pt.tecnico.graph.algorithm.pagerank.ApproximatedPageRankConfig;
import pt.tecnico.graph.algorithm.pagerank.PageRankCsvOutputFormat;
import pt.tecnico.graph.stream.SocketStreamProvider;

/**
 * Created by Renato on 09/04/2016.
 */
public class CitHepPh {
    public static void main(String[] args) {
        String localDir = args[0];
        String remoteDir = args[1];
        int iterations = Integer.parseInt(args[2]);
        double threshold = Double.parseDouble(args[3]);
        int neighborhoodSize = Integer.parseInt(args[4]);
        int outputSize = Integer.parseInt(args[5]);

        ExecutionEnvironment env = ExecutionEnvironment.createRemoteEnvironment("146.193.41.145", 6123,
                "flink-graph-approx-0.1.jar", "flink-graph-algorithms-0.1.jar"
        );

        env.getConfig().disableSysoutLogging().setParallelism(1);

        try {
            Graph<Long, NullValue, NullValue> graph = Graph.fromCsvReader("/home/rrosa/Datasets/Cit-HepPh/Cit-HepPh-init.txt", env)
                    .ignoreCommentsEdges("#")
                    .fieldDelimiterEdges("\t")
                    .keyType(Long.class);

            ApproximatedPageRankConfig config = new ApproximatedPageRankConfig()
                    .setBeta(0.85)
                    .setIterations(iterations)
                    .setUpdatedRatioThreshold(threshold)
                    .setNeighborhoodSize(neighborhoodSize)
                    .setOutputSize(outputSize);

            String outputDir = String.format("%s/Results/CitHepPh-%02.2f-%02d", remoteDir, threshold, neighborhoodSize);
            PageRankCsvOutputFormat outputFormat = new PageRankCsvOutputFormat(outputDir, System.lineSeparator(), ";", false, true);
            outputFormat.setName("approx_pageRank");

            ApproximatedPageRank approximatedPageRank = new ApproximatedPageRank(new SocketStreamProvider("localhost", 1234),
                    graph);
            approximatedPageRank.setConfig(config);
            approximatedPageRank.setOutputFormat(outputFormat);

            String dir = localDir + "/Statistics/CitHepPh";
            approximatedPageRank.setObserver(new ApproximatedPRStatistics(dir, args[6]));

            approximatedPageRank.start();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

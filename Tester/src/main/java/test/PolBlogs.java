package test;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Graph;
import org.apache.flink.types.NullValue;
import pt.tecnico.graph.algorithm.pagerank.ApproximatedPageRank;
import pt.tecnico.graph.algorithm.pagerank.ApproximatedPageRankConfig;
import pt.tecnico.graph.algorithm.pagerank.PageRankCsvOutputFormat;
import pt.tecnico.graph.stream.FileStreamProvider;

/**
 * Created by Renato on 09/04/2016.
 */
public class PolBlogs {
    public static void main(String[] args) {
        String localDir = args[0];
        String remoteDir = args[1];
        int iterations = Integer.parseInt(args[2]);
        int neighborhoodSize = Integer.parseInt(args[3]);
        int outputSize = Integer.parseInt(args[4]);

        ExecutionEnvironment env = ExecutionEnvironment.createRemoteEnvironment("146.193.41.145", 6123,
                "flink-graph-approx-0.1.jar", "flink-graph-algorithms-0.1.jar"
        );

        env.getConfig()
                .disableSysoutLogging()
                .setParallelism(1);
        try {
            Graph<Long, NullValue, NullValue> graph = Graph.fromCsvReader(remoteDir + "/Datasets/polblogs/polblogs_init.csv", env)
                    .ignoreCommentsEdges("#")
                    .fieldDelimiterEdges(";")
                    .keyType(Long.class);

            ApproximatedPageRankConfig config = new ApproximatedPageRankConfig().setBeta(0.85).setIterations(iterations)
                    .setNeighborhoodSize(neighborhoodSize).setOutputSize(outputSize);

            PageRankCsvOutputFormat outputFormat = new PageRankCsvOutputFormat(remoteDir + "/Results/PolBlogs/", System.lineSeparator(), ";", false, true);
            outputFormat.setName("approx_pageRank");

            FileStreamProvider<String> streamProvider = new FileStreamProvider<>(localDir + "/Datasets/polblogs/polblogs_cont.csv", s -> {
                Thread.sleep(10);
                return s;
            });
            ApproximatedPageRank approximatedPageRank = new ApproximatedPageRank(streamProvider, graph);
            approximatedPageRank.setConfig(config);
            approximatedPageRank.setOutputFormat(outputFormat);

            String dir = localDir + "/Statistics/polblogs";
            approximatedPageRank.setObserver(args[5].equals("exact") ? new ExactPRStatistics(dir, args[6]) : new ApproximatedPRStatistics(dir, args[6]));

            approximatedPageRank.start();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}

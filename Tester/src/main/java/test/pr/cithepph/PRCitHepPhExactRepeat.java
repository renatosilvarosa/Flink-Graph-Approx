package test.pr.cithepph;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Graph;
import org.apache.flink.types.NullValue;
import pt.tecnico.graph.algorithm.pagerank.ApproximatePageRank;
import pt.tecnico.graph.algorithm.pagerank.ApproximatePageRankConfig;
import pt.tecnico.graph.algorithm.pagerank.PageRankCsvOutputFormat;
import pt.tecnico.graph.stream.SocketStreamProvider;
import test.pr.ExactRepeatPRStatistics;

public class PRCitHepPhExactRepeat {
    public static void main(String[] args) {
        String localDir = args[0];
        String remoteDir = args[1];
        int iterations = Integer.parseInt(args[2]);
        int outputSize = Integer.parseInt(args[3]);

        ExecutionEnvironment env = ExecutionEnvironment.createRemoteEnvironment("146.193.41.145", 6123,
                "flink-graph-approx-0.3.jar", "flink-graph-algorithms-0.3.jar"
        );

        env.getConfig().disableSysoutLogging().setParallelism(1);

        try {
            Graph<Long, NullValue, NullValue> graph = Graph.fromCsvReader(remoteDir + "/Datasets/Cit-HepPh/Cit-HepPh-init.txt", env)
                    .ignoreCommentsEdges("#")
                    .fieldDelimiterEdges("\t")
                    .keyType(Long.class);

            ApproximatePageRankConfig config = new ApproximatePageRankConfig()
                    .setBeta(0.85)
                    .setIterations(iterations)
                    .setOutputSize(outputSize);

            String outputDir = String.format("%s/Results/PR/CitHepPh-exact-repeat", remoteDir);
            PageRankCsvOutputFormat outputFormat = new PageRankCsvOutputFormat(outputDir, System.lineSeparator(), ";", false, true);
            outputFormat.setName("exact-repeat_PR");

            ApproximatePageRank approximatePageRank = new ApproximatePageRank(new SocketStreamProvider("localhost", 1234),
                    graph);
            approximatePageRank.setConfig(config);
            approximatePageRank.setOutputFormat(outputFormat);

            String dir = localDir + "/Statistics/PR/CitHepPh";
            approximatePageRank.setObserver(new ExactRepeatPRStatistics(dir, args[4]));

            approximatePageRank.start();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

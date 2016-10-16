import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Graph;
import org.apache.flink.types.NullValue;
import pt.tecnico.graph.stream.GraphUpdateTracker;

public class Test {
    public static void main(String[] args) {
        String dir = args[0];
        ExecutionEnvironment env = ExecutionEnvironment.createRemoteEnvironment("146.193.41.145", 6123,
                "flink-graph-approx-0.3.jar", "flink-graph-algorithms-0.3.jar"
        );

        env.getConfig()
                //.disableSysoutLogging()
                .setParallelism(1);
        Graph<Long, NullValue, NullValue> graph = Graph.fromCsvReader(dir + "/Datasets/PolBlogs/polblogs_init.csv", env)
                .ignoreCommentsEdges("#")
                .fieldDelimiterEdges(";")
                .keyType(Long.class);

        GraphUpdateTracker<Long, NullValue, NullValue> updateTracker = new GraphUpdateTracker<>(graph);
        System.out.format("%d;%d%n", 0, updateTracker.getAccumulatedTime());
    }
}

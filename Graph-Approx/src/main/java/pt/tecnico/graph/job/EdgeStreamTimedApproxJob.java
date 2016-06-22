package pt.tecnico.graph.job;

import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.types.NullValue;

import java.util.List;

/**
 * Created by Renato on 30/03/2016.
 */
public class EdgeStreamTimedApproxJob<K, EV, R> extends TimedApproxGraphGob<K, NullValue, EV, Edge<K, EV>, R> {

    public EdgeStreamTimedApproxJob() {
        this(DEFAULT_CONFIGURATION);
    }

    public EdgeStreamTimedApproxJob(GraphJobConfiguration configuration) {
        super(configuration);
    }

    @Override
    protected Graph<K, NullValue, EV> updateGraph(List<Edge<K, EV>> edges) {
        return graph.addVertices(getVertices(edges)).addEdges(edges);
    }

}

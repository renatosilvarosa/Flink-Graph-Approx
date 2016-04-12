package pt.tecnico.graph.job;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.types.NullValue;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by Renato on 07/04/2016.
 */
public class EdgeAddDeleteStreamJob<K, R> extends ApproxGraphGob<K, NullValue, NullValue, Tuple2<String, Edge<K, NullValue>>, R> {
    public EdgeAddDeleteStreamJob(GraphJobConfiguration configuration) {
        super(configuration);
    }

    @Override
    protected Graph<K, NullValue, NullValue> updateGraph(List<Tuple2<String, Edge<K, NullValue>>> updates) {
        final Set<Edge<K, NullValue>> toDelete = new HashSet<>();
        final Set<Edge<K, NullValue>> toAdd = new HashSet<>();

        for (Tuple2<String, Edge<K, NullValue>> update : updates) {
            switch (update.f0) {
                case "A":
                    toAdd.add(update.f1);
                    break;
                case "D":
                    toAdd.remove(update.f1);
                    toDelete.add(update.f1);
                    break;
            }
        }

        return graph.filterOnEdges(new FilterFunction<Edge<K, NullValue>>() {
            @Override
            public boolean filter(Edge<K, NullValue> e) throws Exception {
                return !toDelete.contains(e);
            }
        }).addVertices(getVertices(toAdd)).addEdges(new ArrayList<>(toAdd));
    }
}

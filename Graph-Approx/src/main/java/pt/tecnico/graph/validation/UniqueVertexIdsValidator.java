package pt.tecnico.graph.validation;

import org.apache.flink.graph.Graph;
import org.apache.flink.graph.validation.GraphValidator;

/**
 * Created by Renato on 13/04/2016.
 */
public class UniqueVertexIdsValidator<K, VV, EV> extends GraphValidator<K, VV, EV> {
    @Override
    public boolean validate(Graph<K, VV, EV> graph) throws Exception {
        long total = graph.getVertexIds().count();
        long distinct = graph.getVertexIds().distinct().count();

        return total == distinct;
    }
}

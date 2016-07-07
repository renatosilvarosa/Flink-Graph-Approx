package pt.tecnico.graph.validation;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.validation.GraphValidator;

/**
 * Created by Renato on 29/06/2016.
 */
public class LongPositiveIdsValidator<K, VV, EV> extends GraphValidator<K, VV, EV> {

    private static TypeInformation<Long> longTypeInfo = TypeInformation.of(Long.class);

    private DataSet<K> offendingIds;

    @Override
    public boolean validate(Graph<K, VV, EV> graph) throws Exception {
        DataSet<K> vertexIds = graph.getVertexIds();

        if (!vertexIds.getType().equals(longTypeInfo)) {
            offendingIds = vertexIds;
            return false;
        }

        offendingIds = vertexIds.filter(id -> ((Long) id) <= 0);

        return offendingIds.count() == 0;
    }

    public DataSet<K> getOffendingIds() {
        return offendingIds;
    }
}

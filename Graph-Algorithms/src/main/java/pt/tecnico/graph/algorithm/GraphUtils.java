package pt.tecnico.graph.algorithm;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.util.Collector;

public class GraphUtils {

    public static <K, VV, EV> DataSet<K> expandedVertexIds(Graph<K, VV, EV> originalGraph, DataSet<K> originalVertexIds, int level) throws Exception {
        DataSet<K> expandedIds = originalVertexIds;
        VertexKeySelector<K> keySelector = new VertexKeySelector<>(originalGraph.getVertexIds().getType());
        while (level > 0) {
            DataSet<K> firstNeighbours = expandedIds
                    .join(originalGraph.getEdges())
                    .where(keySelector).equalTo(0)
                    .with((id, e) -> e.getTarget())
                    .returns(expandedIds.getType())
                    .name("Neighbours level " + level);

            expandedIds = expandedIds.union(firstNeighbours).distinct().name("Expanded level " + level);
            level--;
        }

        return expandedIds;
    }

    public static <K, VV, EV> DataSet<Edge<K, EV>> selectEdges(Graph<K, VV, EV> originalGraph, DataSet<Vertex<K, VV>> vertices) {
        return vertices
                .joinWithHuge(originalGraph.getEdges())
                .where(0).equalTo(0)
                .with((source, edge) -> edge)
                .returns(originalGraph.getEdges().getType())
                .join(vertices)
                .where(1).equalTo(0)
                .with((e, v) -> e)
                .returns(originalGraph.getEdges().getType())
                .distinct(0, 1);
    }

    public static <K, VV, EV> DataSet<Edge<K, EV>> selectEdgesWithIds(Graph<K, VV, EV> originalGraph, DataSet<K> vertexIds) {
        VertexKeySelector<K> keySelector = new VertexKeySelector<>(vertexIds.getType());
        return vertexIds
                .joinWithHuge(originalGraph.getEdges())
                .where(keySelector).equalTo(0)
                .with((source, edge) -> edge)
                .returns(originalGraph.getEdges().getType())
                .join(vertexIds)
                .where(1).equalTo(keySelector)
                .with((e, v) -> e)
                .returns(originalGraph.getEdges().getType())
                .distinct(0, 1);
    }

    public static <K, VV, EV> DataSet<Edge<K, EV>> externalEdges(Graph<K, VV, EV> originalGraph, DataSet<Edge<K, EV>> edgesToBeRemoved) {
        return originalGraph.getEdges().coGroup(edgesToBeRemoved)
                .where(0, 1).equalTo(0, 1)
                .with((Iterable<Edge<K, EV>> edge, Iterable<Edge<K, EV>> edgeToBeRemoved, Collector<Edge<K, EV>> out) -> {
                    if (!edgeToBeRemoved.iterator().hasNext()) {
                        for (Edge<K, EV> next : edge) {
                            out.collect(next);
                        }
                    }
                }).returns(originalGraph.getEdges().getType());
    }

    public static class VertexKeySelector<K> implements KeySelector<K, K>, ResultTypeQueryable<K> {
        final TypeInformation<K> type;

        public VertexKeySelector(TypeInformation<K> type) {
            this.type = type;
        }

        @Override
        public K getKey(K value) throws Exception {
            return value;
        }

        @Override
        public TypeInformation<K> getProducedType() {
            return type;
        }
    }

    public static class EdgeToTuple2<K, EV> implements MapFunction<Edge<K, EV>, Tuple2<K, K>> {
        @Override
        public Tuple2<K, K> map(Edge<K, EV> edge) throws Exception {
            return Tuple2.of(edge.getSource(), edge.getTarget());
        }
    }
}

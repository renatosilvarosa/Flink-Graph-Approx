package pt.tecnico.graph.algorithm;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;
import org.objenesis.Objenesis;
import org.objenesis.ObjenesisStd;

import java.util.Collections;

/**
 * Utility methods for graphs in Flink
 *
 * @author Renato Rosa
 */
public class GraphUtils {

    static <T> DataSet<T> emptyDataSet(ExecutionEnvironment env, TypeInformation<T> typeInformation) {
        return env.fromCollection(Collections.emptyList(), typeInformation);
    }

    static <T> DataSet<T> emptyDataSet(ExecutionEnvironment env, TypeHint<T> typeHint) {
        return env.fromCollection(Collections.emptyList(), typeHint.getTypeInfo());
    }

    static <T> DataSet<T> emptyDataSet(ExecutionEnvironment env, Class<T> clazz) {
        return env.fromCollection(Collections.emptyList(), TypeInformation.of(clazz));
    }

    static <K, VV, EV> Graph<K, VV, EV> emptyGraph(ExecutionEnvironment env, Class<K> keyType,
                                                   Class<VV> vertexType, Class<EV> edgeType) {
        Objenesis instantiator = new ObjenesisStd(true);
        K k = instantiator.newInstance(keyType);
        VV vv = instantiator.newInstance(vertexType);
        EV ev = instantiator.newInstance(edgeType);

        TypeInformation<Vertex<K, VV>> vertexInfo = TypeExtractor.getForObject(new Vertex<>(k, vv));
        TypeInformation<Edge<K, EV>> edgeInfo = TypeExtractor.getForObject(new Edge<>(k, k, ev));

        return Graph.fromDataSet(emptyDataSet(env, vertexInfo), emptyDataSet(env, edgeInfo), env);

    }

    static <K> Graph<K, NullValue, NullValue> emptyGraph(ExecutionEnvironment env, Class<K> keyType) {
        Objenesis instantiator = new ObjenesisStd(true);
        K k = instantiator.newInstance(keyType);

        TypeInformation<Vertex<K, NullValue>> vertexInfo = TypeExtractor.getForObject(new Vertex<>(k, NullValue.getInstance()));
        TypeInformation<Edge<K, NullValue>> edgeInfo = TypeExtractor.getForObject(new Edge<>(k, k, NullValue.getInstance()));

        return Graph.fromDataSet(emptyDataSet(env, vertexInfo), emptyDataSet(env, edgeInfo), env);
    }

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

    private static class VertexKeySelector<K> implements KeySelector<K, K>, ResultTypeQueryable<K> {
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

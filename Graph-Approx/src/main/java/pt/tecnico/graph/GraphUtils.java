package pt.tecnico.graph;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.EdgeDirection;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.objenesis.Objenesis;
import org.objenesis.ObjenesisStd;
import pt.tecnico.graph.stream.GraphUpdateTracker;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public interface GraphUtils {
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

    static <K> Set<K> updatedAboveThresholdIds(Map<K, GraphUpdateTracker.UpdateInfo> infoMap, double threshold, EdgeDirection direction) {
        if (threshold <= 0.0) {
            return allUpdatedIds(infoMap, direction);
        }

        Predicate<Map.Entry<K, GraphUpdateTracker.UpdateInfo>> pred;
        switch (direction) {
            case IN:
                pred = e -> {
                    GraphUpdateTracker.UpdateInfo i = e.getValue();
                    return degreeUpdateRatio(i.prevInDegree, i.currInDegree) > threshold;
                };
                break;
            case OUT:
                pred = e -> {
                    GraphUpdateTracker.UpdateInfo i = e.getValue();
                    return degreeUpdateRatio(i.prevOutDegree, i.currOutDegree) > threshold;
                };
                break;
            default:
                pred = e -> {
                    GraphUpdateTracker.UpdateInfo i = e.getValue();
                    return degreeUpdateRatio(i.prevInDegree, i.currInDegree) > threshold ||
                            degreeUpdateRatio(i.prevOutDegree, i.currOutDegree) > threshold;
                };
                break;
        }

        return infoMap.entrySet().stream()
                .filter(pred)
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());
    }

    static double degreeUpdateRatio(long prevDeg, long currDeg) {
        assert prevDeg >= 0 && currDeg >= 0 : "Negative degrees";
        if (prevDeg == 0) {
            return Double.POSITIVE_INFINITY;
        }

        return Math.abs((double) currDeg / prevDeg - 1.0);
    }

    static <K> Set<K> allUpdatedIds(Map<K, GraphUpdateTracker.UpdateInfo> infoMap, EdgeDirection direction) {
        Set<K> set1 = new HashSet<>();
        Set<K> set2 = new HashSet<>();

        if (direction == EdgeDirection.IN || direction == EdgeDirection.ALL) {
            set1 = infoMap.entrySet().stream()
                    .filter(e -> e.getValue().currInDegree != e.getValue().prevInDegree)
                    .map(Map.Entry::getKey)
                    .collect(Collectors.toSet());
        }

        if (direction == EdgeDirection.OUT || direction == EdgeDirection.ALL) {
            set2 = infoMap.entrySet().stream()
                    .filter(e -> e.getValue().currOutDegree != e.getValue().prevOutDegree)
                    .map(Map.Entry::getKey)
                    .collect(Collectors.toSet());
        }

        set1.addAll(set2);
        return set1;
    }
}

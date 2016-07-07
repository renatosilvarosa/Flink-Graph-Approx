package pt.tecnico.graph.job;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.graph.EdgeDirection;
import org.apache.flink.graph.Graph;
import org.apache.flink.types.LongValue;
import pt.tecnico.graph.GraphUtils;

import java.util.*;
import java.util.stream.Collectors;

public class DegreeTracker<K> {
    private final ExecutionEnvironment env;
    private final TypeInformation<K> typeInformation;
    private Map<K, Tuple2<LongValue, LongValue>> inDegrees = new HashMap<>();
    private Map<K, Tuple2<LongValue, LongValue>> outDegrees = new HashMap<>();

    public DegreeTracker(Graph<K, ?, ?> initialGraph) {
        env = initialGraph.getContext();
        typeInformation = initialGraph.getVertexIds().getType();
        try {
            outDegrees = initialGraph.outDegrees().collect().stream()
                    .collect(Collectors.toMap(d -> d.f0, d -> Tuple2.of(d.f1.copy(), d.f1.copy())));
            inDegrees = initialGraph.inDegrees().collect().stream()
                    .collect(Collectors.toMap(d -> d.f0, d -> Tuple2.of(d.f1.copy(), d.f1.copy())));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static Tuple2<Long, Long> sum(Tuple2<Long, Long> t1, Tuple2<Long, Long> t2) {
        return Tuple2.of(t1.f0 + t2.f0, t1.f1 + t2.f1);
    }

    private static Tuple2<Long, Long> subtract(Tuple2<Long, Long> t1, Tuple2<Long, Long> t2) {
        return Tuple2.of(t1.f0 - t2.f0, t1.f1 - t2.f1);
    }

    private static LongValue longValue(long l) {
        return new LongValue(l);
    }

    public long numberOfVertices() {
        return inDegrees.size();
    }

    public void addEdge(K source, K target) {
        incrementOutDegree(source);
        incrementInDegree(target);
    }

    public void removeEdge(K source, K target) {
        decrementOutDegree(source);
        decrementInDegree(target);
    }

    public DataSet<K> updatedAboveThresholdVertexIds(double threshold, EdgeDirection direction) {
        DataSet<K> set1 = GraphUtils.emptyDataSet(env, typeInformation);
        DataSet<K> set2 = GraphUtils.emptyDataSet(env, typeInformation);

        if (direction == EdgeDirection.IN || direction == EdgeDirection.ALL) {
            set1 = updatedAboveThreshold(inDegrees, threshold);
        }

        if (direction == EdgeDirection.OUT || direction == EdgeDirection.ALL) {
            set2 = updatedAboveThreshold(outDegrees, threshold);
        }

        return set1.union(set2).distinct();
    }

    public DataSet<K> allUpdatedVertexIds(EdgeDirection direction) {
        DataSet<K> set1 = GraphUtils.emptyDataSet(env, typeInformation);
        DataSet<K> set2 = GraphUtils.emptyDataSet(env, typeInformation);

        if (direction == EdgeDirection.IN || direction == EdgeDirection.ALL) {
            set1 = updated(inDegrees);
        }

        if (direction == EdgeDirection.OUT || direction == EdgeDirection.ALL) {
            set2 = updated(outDegrees);
        }

        return set1.union(set2).distinct();
    }

    public void reset(DataSet<K> ids) throws Exception {
        List<K> list = ids.collect();
        for (K id : list) {
            if (inDegrees.containsKey(id)) {
                Tuple2<LongValue, LongValue> mapping = inDegrees.get(id);
                mapping.f0.setValue(mapping.f1.getValue());
            }

            if (outDegrees.containsKey(id)) {
                Tuple2<LongValue, LongValue> mapping = outDegrees.get(id);
                mapping.f0.setValue(mapping.f1.getValue());
            }
        }
    }

    private DataSet<K> updatedAboveThreshold(Map<K, Tuple2<LongValue, LongValue>> degrees, double threshold) {
        List<Map.Entry<K, Tuple2<LongValue, LongValue>>> copy = new ArrayList<>(degrees.entrySet());
        return env.fromCollection(copy)
                .filter(new FilterFunction<Map.Entry<K, Tuple2<LongValue, LongValue>>>() {
                    @Override
                    public boolean filter(Map.Entry<K, Tuple2<LongValue, LongValue>> e) throws Exception {
                        Tuple2<LongValue, LongValue> value = e.getValue();
                        if (value.f0.getValue() == 0) {
                            return value.f1.getValue() > 0;
                        }

                        return ((value.f1.getValue() / value.f0.getValue()) - 1) >= threshold;
                    }
                }).map(new KeyMapper<>(typeInformation));
    }

    private DataSet<K> updated(Map<K, Tuple2<LongValue, LongValue>> degrees) {
        List<Map.Entry<K, Tuple2<LongValue, LongValue>>> copy = new ArrayList<>(degrees.entrySet());
        return env.fromCollection(copy)
                .filter(new FilterFunction<Map.Entry<K, Tuple2<LongValue, LongValue>>>() {
                    @Override
                    public boolean filter(Map.Entry<K, Tuple2<LongValue, LongValue>> e) throws Exception {
                        Tuple2<LongValue, LongValue> value = e.getValue();
                        return !Objects.equals(value.f0, value.f1);
                    }
                }).map(new KeyMapper<>(typeInformation));
    }

    private void incrementInDegree(K id) {
        Tuple2<LongValue, LongValue> mapping = inDegrees.computeIfAbsent(id, k -> Tuple2.of(longValue(0L), longValue(0L)));
        mapping.f1.setValue(mapping.f1.getValue() + 1);
    }

    private void incrementOutDegree(K id) {
        Tuple2<LongValue, LongValue> mapping = outDegrees.computeIfAbsent(id, k -> Tuple2.of(longValue(0L), longValue(0L)));
        mapping.f1.setValue(mapping.f1.getValue() + 1);
    }

    private void decrementInDegree(K id) {
        Tuple2<LongValue, LongValue> mapping = inDegrees.computeIfAbsent(id, k -> Tuple2.of(longValue(0L), longValue(0L)));
        mapping.f1.setValue(mapping.f1.getValue() - 1);
    }

    private void decrementOutDegree(K id) {
        Tuple2<LongValue, LongValue> mapping = outDegrees.computeIfAbsent(id, k -> Tuple2.of(longValue(0L), longValue(0L)));
        mapping.f1.setValue(mapping.f1.getValue() - 1);
    }

    private static class KeyMapper<K> implements MapFunction<Map.Entry<K, Tuple2<LongValue, LongValue>>, K>, ResultTypeQueryable<K> {
        private final TypeInformation<K> typeInformation;

        private KeyMapper(TypeInformation<K> typeInformation) {
            this.typeInformation = typeInformation;
        }

        @Override
        public K map(Map.Entry<K, Tuple2<LongValue, LongValue>> entry) throws Exception {
            return entry.getKey();
        }

        @Override
        public TypeInformation<K> getProducedType() {
            return typeInformation;
        }
    }
}

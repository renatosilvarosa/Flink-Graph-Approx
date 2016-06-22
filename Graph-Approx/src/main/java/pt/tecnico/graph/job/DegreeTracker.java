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
import pt.tecnico.graph.GraphUtils;

import java.util.*;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

public class DegreeTracker<K> {
    private final ExecutionEnvironment env;
    private final TypeInformation<K> typeInformation;
    private Map<K, Tuple2<Long, Long>> inDegrees = new HashMap<>();
    private Map<K, Tuple2<Long, Long>> outDegrees = new HashMap<>();

    public DegreeTracker(Graph<K, ?, ?> initialGraph) {
        env = initialGraph.getContext();
        typeInformation = initialGraph.getVertexIds().getType();
        try {
            outDegrees = initialGraph.outDegrees().collect().stream()
                    .collect(Collectors.toMap(d -> d.f0, d -> Tuple2.of(d.f1, d.f1)));
            inDegrees = initialGraph.inDegrees().collect().stream()
                    .collect(Collectors.toMap(d -> d.f0, d -> Tuple2.of(d.f1, d.f1)));
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
        BiFunction<K, Tuple2<Long, Long>, Tuple2<Long, Long>> func = (k, v) -> Tuple2.of(v.f1, v.f1);
        for (K id : list) {
            inDegrees.computeIfPresent(id, func);
            outDegrees.computeIfPresent(id, func);
        }
    }

    private DataSet<K> updatedAboveThreshold(Map<K, Tuple2<Long, Long>> degrees, double threshold) {
        List<Map.Entry<K, Tuple2<Long, Long>>> copy = new ArrayList<>(degrees.entrySet());
        return env.fromCollection(copy)
                .filter(new FilterFunction<Map.Entry<K, Tuple2<Long, Long>>>() {
                    @Override
                    public boolean filter(Map.Entry<K, Tuple2<Long, Long>> e) throws Exception {
                        Tuple2<Long, Long> value = e.getValue();
                        if (value.f0 == 0) {
                            return value.f1 > 0;
                        }

                        return ((value.f1 / value.f0) - 1) >= threshold;
                    }
                }).map(new KeyMapper<>(typeInformation));
    }

    private DataSet<K> updated(Map<K, Tuple2<Long, Long>> degrees) {
        List<Map.Entry<K, Tuple2<Long, Long>>> copy = new ArrayList<>(degrees.entrySet());
        return env.fromCollection(copy)
                .filter(new FilterFunction<Map.Entry<K, Tuple2<Long, Long>>>() {
                    @Override
                    public boolean filter(Map.Entry<K, Tuple2<Long, Long>> e) throws Exception {
                        Tuple2<Long, Long> value = e.getValue();
                        return !Objects.equals(value.f0, value.f1);
                    }
                }).map(new KeyMapper<>(typeInformation));
    }

    private void incrementInDegree(K id) {
        inDegrees.merge(id, Tuple2.of(0L, 1L), DegreeTracker::sum);
    }

    private void incrementOutDegree(K id) {
        outDegrees.merge(id, Tuple2.of(0L, 1L), DegreeTracker::sum);
    }

    private void decrementInDegree(K id) {
        inDegrees.merge(id, Tuple2.of(0L, 1L), DegreeTracker::subtract);
    }

    private void decrementOutDegree(K id) {
        outDegrees.merge(id, Tuple2.of(0L, 1L), DegreeTracker::subtract);
    }

    private static class KeyMapper<K> implements MapFunction<Map.Entry<K, Tuple2<Long, Long>>, K>, ResultTypeQueryable<K> {
        private final TypeInformation<K> typeInformation;

        private KeyMapper(TypeInformation<K> typeInformation) {
            this.typeInformation = typeInformation;
        }

        @Override
        public K map(Map.Entry<K, Tuple2<Long, Long>> entry) throws Exception {
            return entry.getKey();
        }

        @Override
        public TypeInformation<K> getProducedType() {
            return typeInformation;
        }
    }
}

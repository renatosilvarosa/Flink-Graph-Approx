package pt.tecnico.graph.stream;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.EdgeDirection;
import org.apache.flink.graph.Graph;
import org.apache.flink.types.LongValue;

import java.io.Serializable;
import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class GraphUpdateTracker<K> implements Serializable {
    private final Set<K> verticesToAdd = new HashSet<>();
    private final Set<K> verticesToRemove = new HashSet<>();
    private Map<K, UpdateInfo> infoMap = new HashMap<>();
    private long edgeAdditions;
    private long edgeRemovals;
    private long currentNumberOfVertices;
    private long currentNumberOfEdges;

    public GraphUpdateTracker(Graph<K, ?, ?> initialGraph) {
        try {
            infoMap = initialGraph.inDegrees()
                    .join(initialGraph.outDegrees())
                    .where(0).equalTo(0)
                    .with(new JoinFunction<Tuple2<K, LongValue>, Tuple2<K, LongValue>, Tuple3<K, Long, Long>>() {
                        @Override
                        public Tuple3<K, Long, Long> join(Tuple2<K, LongValue> inDeg, Tuple2<K, LongValue> outDeg) throws Exception {
                            return Tuple3.of(inDeg.f0, inDeg.f1.getValue(), outDeg.f1.getValue());
                        }
                    }).collect().parallelStream()
                    .collect(Collectors.toMap(t -> t.f0, t -> new UpdateInfo(t.f1, t.f2)));
            currentNumberOfVertices = initialGraph.numberOfVertices();
            currentNumberOfEdges = initialGraph.numberOfEdges();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public GraphUpdateStatistics getUpdateStatistics() {
        return new GraphUpdateStatistics(
                verticesToAdd.size(), verticesToRemove.size(), edgeAdditions, edgeRemovals,
                numberOfUpdatedVertices(), getCurrentNumberOfVertices(), getCurrentNumberOfEdges()
        );
    }

    public Set<K> getVerticesToAdd() {
        return new HashSet<>(verticesToAdd);
    }

    public Set<K> getVerticesToRemove() {
        return new HashSet<>(verticesToRemove);
    }

    public long getCurrentNumberOfVertices() {
        return currentNumberOfVertices + verticesToAdd.size() - verticesToRemove.size();
    }

    public long getCurrentNumberOfEdges() {
        return currentNumberOfEdges + edgeAdditions - edgeRemovals;
    }

    public void addEdge(K source, K target) {
        UpdateInfo info = infoMap.computeIfAbsent(source, k -> {
            verticesToAdd.add(source);
            verticesToRemove.remove(source);
            return new UpdateInfo(0, 0);
        });
        info.nUpdates++;
        info.currOutDegree++;

        info = infoMap.computeIfAbsent(source, k -> {
            verticesToAdd.add(target);
            verticesToRemove.remove(target);
            return new UpdateInfo(0, 0);
        });
        info.nUpdates++;
        info.currInDegree++;
        edgeAdditions++;
    }

    public void removeEdge(K source, K target) {
        if (infoMap.containsKey(source)) {
            UpdateInfo info = infoMap.get(source);
            info.nUpdates++;
            info.currOutDegree--;
            if (info.currInDegree == 0 && info.currOutDegree == 0) {
                verticesToRemove.add(source);
                verticesToAdd.remove(source);
            }
        }

        if (infoMap.containsKey(target)) {
            UpdateInfo info = infoMap.get(target);
            info.nUpdates++;
            info.currInDegree--;
            if (info.currInDegree == 0 && info.currOutDegree == 0) {
                verticesToRemove.add(target);
                verticesToAdd.remove(target);
            }
        }

        edgeRemovals++;
    }

    public Set<K> updatedAboveThresholdVertexIds(double threshold, EdgeDirection direction) {
        Predicate<Map.Entry<K, UpdateInfo>> pred;
        switch (direction) {
            case IN:
                pred = e -> {
                    UpdateInfo i = e.getValue();
                    return i.prevInDegree != 0 ? i.currInDegree / i.prevInDegree > threshold : i.currInDegree > 0;
                };
                break;
            case OUT:
                pred = e -> {
                    UpdateInfo i = e.getValue();
                    return i.prevOutDegree != 0 ? i.currOutDegree / i.prevOutDegree > threshold : i.currOutDegree > 0;
                };
                break;
            default:
                pred = e -> {
                    UpdateInfo i = e.getValue();
                    return i.prevInDegree != 0 ? i.currInDegree / i.prevInDegree > threshold : i.currInDegree > 0 ||
                            i.prevOutDegree != 0 ? i.currOutDegree / i.prevOutDegree > threshold : i.currOutDegree > 0;
                };
                break;
        }

        return infoMap.entrySet().stream()
                .filter(pred)
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());
    }

    public long numberOfUpdatedVertices() {
        return infoMap.values().parallelStream()
                .filter(i -> i.nUpdates > 0)
                .collect(Collectors.counting());
    }

    public Set<K> allUpdatedVertexIds(EdgeDirection direction) {
        Set<K> set1 = new HashSet<>();
        Set<K> set2 = new HashSet<>();

        if (direction == EdgeDirection.IN || direction == EdgeDirection.ALL) {
            set1 = infoMap.entrySet().stream()
                    .filter(e -> e.getValue().currInDegree != e.getValue().prevInDegree)
                    .map(Map.Entry::getKey)
                    .collect(Collectors.toSet());
        }

        if (direction == EdgeDirection.IN || direction == EdgeDirection.ALL) {
            set2 = infoMap.entrySet().stream()
                    .filter(e -> e.getValue().currOutDegree != e.getValue().prevOutDegree)
                    .map(Map.Entry::getKey)
                    .collect(Collectors.toSet());
        }

        set1.addAll(set2);
        return set1;
    }

    public void reset(Collection<K> ids) throws Exception {
        ids.stream().forEach(id -> infoMap.get(id).reset());
    }

    public long numberOfVertices() {
        return infoMap.size();
    }

    private static class UpdateInfo implements Serializable {
        long nUpdates;
        long prevInDegree;
        long currInDegree;
        long prevOutDegree;
        long currOutDegree;

        UpdateInfo(long inDegree, long outDegree) {
            currInDegree = prevInDegree = inDegree;
            currOutDegree = prevOutDegree = outDegree;
            nUpdates = 0;
        }

        void reset() {
            nUpdates = 0;
            prevInDegree = currInDegree;
            prevOutDegree = currOutDegree;
        }
    }
}

package pt.tecnico.graph.stream;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.EdgeDirection;
import org.apache.flink.graph.Graph;
import org.apache.flink.types.LongValue;

import java.io.Serializable;
import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class GraphUpdateTracker<K, VV, EV> implements Serializable {
    private final Set<K> verticesToAdd = new HashSet<>();
    private final Set<K> verticesToRemove = new HashSet<>();
    private final Set<Edge<K, EV>> edgesToAdd = new HashSet<>();
    private final Set<Edge<K, EV>> edgesToRemove = new HashSet<>();

    private Map<K, UpdateInfo> infoMap = new HashMap<>();

    private long currentNumberOfVertices;
    private long currentNumberOfEdges;

    private long accumulatedTime;

    public GraphUpdateTracker(Graph<K, VV, EV> initialGraph) {
        accumulatedTime = 0L;
        try {
            long start = System.currentTimeMillis();
            List<Tuple2<K, UpdateInfo>> degrees = initialGraph.inDegrees()
                    .join(initialGraph.outDegrees())
                    .where(0).equalTo(0)
                    .with(new JoinFunction<Tuple2<K, LongValue>, Tuple2<K, LongValue>, Tuple2<K, UpdateInfo>>() {
                        @Override
                        public Tuple2<K, UpdateInfo> join(Tuple2<K, LongValue> inDeg, Tuple2<K, LongValue> outDeg) throws Exception {
                            return Tuple2.of(inDeg.f0, new UpdateInfo(inDeg.f1.getValue(), outDeg.f1.getValue()));
                        }
                    }).collect();

            infoMap = degrees.stream()
                    .collect(Collectors.toMap(t -> t.f0, t -> t.f1));
            currentNumberOfVertices = initialGraph.numberOfVertices();
            currentNumberOfEdges = initialGraph.numberOfEdges();
            accumulatedTime += System.currentTimeMillis() - start;
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static <K> Set<K> updatedAboveThresholdIds(Map<K, GraphUpdateTracker.UpdateInfo> infoMap, double threshold, EdgeDirection direction) {
        if (threshold <= 0.0) {
            return allUpdatedIds(infoMap, direction);
        }

        Predicate<Map.Entry<K, UpdateInfo>> pred;
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

    public static <K> Set<K> allUpdatedIds(Map<K, GraphUpdateTracker.UpdateInfo> infoMap, EdgeDirection direction) {
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

    public GraphUpdates<K, EV> getGraphUpdates() {
        return new GraphUpdates<>(verticesToAdd, verticesToRemove, edgesToAdd, edgesToRemove);
    }

    public GraphUpdateStatistics getUpdateStatistics() {
        return new GraphUpdateStatistics(verticesToAdd.size(), verticesToRemove.size(),
                edgesToAdd.size(),
                edgesToRemove.size(),
                infoMap.values().parallelStream()
                        .filter(i -> i.nUpdates > 0)
                        .collect(Collectors.counting()),
                currentNumberOfVertices + verticesToAdd.size() - verticesToRemove.size(),
                currentNumberOfEdges + edgesToAdd.size() - edgesToRemove.size());
    }

    public Map<K, UpdateInfo> getUpdateInfos() {
        return Collections.unmodifiableMap(infoMap);
    }

    void addEdge(Edge<K, EV> edge) {
        long start = System.currentTimeMillis();
        edgesToAdd.add(edge);
        edgesToRemove.remove(edge);
        UpdateInfo info = putOrGetInfo(edge.getSource());
        info.nUpdates++;
        info.currOutDegree++;

        info = putOrGetInfo(edge.getTarget());
        info.nUpdates++;
        info.currInDegree++;
        accumulatedTime += System.currentTimeMillis() - start;
    }

    private UpdateInfo putOrGetInfo(K vertex) {
        return infoMap.computeIfAbsent(vertex, k -> {
            verticesToAdd.add(vertex);
            verticesToRemove.remove(vertex);
            return new UpdateInfo(0, 0);
        });
    }

    void removeEdge(Edge<K, EV> edge) {
        edgesToRemove.add(edge);
        edgesToAdd.remove(edge);
        if (infoMap.containsKey(edge.getSource())) {
            UpdateInfo info = infoMap.get(edge.getSource());
            info.nUpdates++;
            info.currOutDegree--;
            checkRemove(edge.getSource());
        }

        if (infoMap.containsKey(edge.getTarget())) {
            UpdateInfo info = infoMap.get(edge.getTarget());
            info.nUpdates++;
            info.currInDegree--;
            checkRemove(edge.getTarget());
        }
    }

    private void checkRemove(K vertex) {
        UpdateInfo info = infoMap.get(vertex);
        if (info.currInDegree == 0 && info.currOutDegree == 0) {
            verticesToRemove.add(vertex);
            verticesToAdd.remove(vertex);
            infoMap.remove(vertex);
        }
    }

    public void resetUpdates() {
        verticesToAdd.clear();
        verticesToRemove.clear();
        edgesToAdd.clear();
        edgesToRemove.clear();
        accumulatedTime = 0;
    }

    public void reset(Collection<K> ids) throws Exception {
        ids.forEach(id -> infoMap.get(id).reset());
    }

    public void resetAll() {
        infoMap.values().forEach(UpdateInfo::reset);
        accumulatedTime = 0;
    }

    public long getAccumulatedTime() {
        return accumulatedTime;
    }

    public static class UpdateInfo implements Serializable {
        public long nUpdates;
        public long prevInDegree;
        public long currInDegree;
        public long prevOutDegree;
        public long currOutDegree;

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

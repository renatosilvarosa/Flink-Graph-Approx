package pt.tecnico.graph.stream;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.EdgeDirection;
import org.apache.flink.graph.Graph;
import org.apache.flink.types.LongValue;
import pt.tecnico.graph.GraphUtils;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

public class GraphUpdateTracker<K, VV, EV> implements Serializable {
    private final Set<K> verticesToAdd = new HashSet<>();
    private final Set<K> verticesToRemove = new HashSet<>();
    private final Set<Edge<K, EV>> edgesToAdd = new HashSet<>();
    private final Set<Edge<K, EV>> edgesToRemove = new HashSet<>();

    private Map<K, UpdateInfo> infoMap = new HashMap<>();

    private long currentNumberOfVertices;
    private long currentNumberOfEdges;

    public GraphUpdateTracker(Graph<K, VV, EV> initialGraph) {
        try {
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
        } catch (Exception e) {
            e.printStackTrace();
        }
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
        edgesToAdd.add(edge);
        edgesToRemove.remove(edge);
        UpdateInfo info = putOrGetInfo(edge.getSource());
        info.nUpdates++;
        info.currOutDegree++;

        info = putOrGetInfo(edge.getTarget());
        info.nUpdates++;
        info.currInDegree++;
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

    Set<K> updatedAboveThresholdIds(double threshold, EdgeDirection direction) {
        return GraphUtils.updatedAboveThresholdIds(infoMap, threshold, direction);
    }

    public void resetUpdates() {
        verticesToAdd.clear();
        verticesToRemove.clear();
        edgesToAdd.clear();
        edgesToRemove.clear();
    }

    public void reset(Collection<K> ids) throws Exception {
        ids.forEach(id -> infoMap.get(id).reset());
    }

    public void resetAll() {
        infoMap.values().forEach(UpdateInfo::reset);
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

package pt.tecnico.graph.job;

import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Created by Renato on 07/04/2016.
 */
public abstract class UpdateTracker<K> implements Serializable {
    private final Map<K, Integer> updateCounter = new HashMap<>();

    public void logUpdate(Edge<K, ?> edge) {
        int nUpdates = updateCounter.getOrDefault(edge.getSource(), 0) + 1;
        updateCounter.put(edge.getSource(), nUpdates);
        nUpdates = updateCounter.getOrDefault(edge.getTarget(), 0) + 1;
        updateCounter.put(edge.getTarget(), nUpdates);
    }

    public void logUpdate(Vertex<K, ?> vertex) {
        updateCounter.put(vertex.getId(), updateCounter.getOrDefault(vertex.getId(), 0) + 1);
    }

    public Set<K> getUpdatedVertexIds(int threshold) {
        return updateCounter.entrySet().parallelStream()
                .filter(e -> e.getValue() >= threshold)
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());
    }

    public void resetAll() {
        updateCounter.clear();
    }

    public void reset(Collection<K> ids) {
        updateCounter.keySet().removeAll(ids);
    }
}

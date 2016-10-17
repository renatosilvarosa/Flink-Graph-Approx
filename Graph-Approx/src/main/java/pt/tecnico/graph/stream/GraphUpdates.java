package pt.tecnico.graph.stream;

import org.apache.flink.graph.Edge;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

/**
 * Container for graph updates
 *
 * @param <K>
 * @param <EV>
 * @author Renato Rosa
 */
public class GraphUpdates<K, EV> implements Serializable {
    public final Set<K> verticesToAdd;
    public final Set<K> verticesToRemove;
    public final Set<Edge<K, EV>> edgesToAdd;
    public final Set<Edge<K, EV>> edgesToRemove;

    GraphUpdates(Set<K> verticesToAdd, Set<K> verticesToRemove, Set<Edge<K, EV>> edgesToAdd, Set<Edge<K, EV>> edgesToRemove) {
        this.verticesToAdd = new HashSet<>(verticesToAdd);
        this.verticesToRemove = new HashSet<>(verticesToRemove);
        this.edgesToAdd = new HashSet<>(edgesToAdd);
        this.edgesToRemove = new HashSet<>(edgesToRemove);
    }
}

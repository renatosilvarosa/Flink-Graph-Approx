package pt.tecnico.graph.stream;

/**
 * Created by Renato on 12/07/2016.
 */
public class GraphUpdateStatistics {
    private long addedVertices;
    private long removedVertices;
    private long addedEdges;
    private long removedEdges;
    private long updatedVertices;
    private long totalVertices;
    private long totalEdges;

    GraphUpdateStatistics(long addedVertices, long removedVertices, long addedEdges, long removedEdges, long updatedVertices, long totalVertices, long totalEdges) {
        this.addedVertices = addedVertices;
        this.removedVertices = removedVertices;
        this.addedEdges = addedEdges;
        this.removedEdges = removedEdges;
        this.updatedVertices = updatedVertices;
        this.totalVertices = totalVertices;
        this.totalEdges = totalEdges;
    }

    public long getAddedVertices() {
        return addedVertices;
    }

    public long getRemovedVertices() {
        return removedVertices;
    }

    public long getAddedEdges() {
        return addedEdges;
    }

    public long getRemovedEdges() {
        return removedEdges;
    }

    public long getUpdatedVertices() {
        return updatedVertices;
    }

    public long getTotalVertices() {
        return totalVertices;
    }

    public long getTotalEdges() {
        return totalEdges;
    }
}

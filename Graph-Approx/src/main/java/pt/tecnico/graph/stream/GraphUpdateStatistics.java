package pt.tecnico.graph.stream;

import org.apache.flink.api.java.tuple.Tuple7;

public class GraphUpdateStatistics extends Tuple7<Long, Long, Long, Long, Long, Long, Long> {
    public GraphUpdateStatistics(long addedVertices, long removedVertices, long addedEdges, long removedEdges, long updatedVertices, long totalVertices, long totalEdges) {
        this.f0 = addedVertices;
        this.f1 = removedVertices;
        this.f2 = addedEdges;
        this.f3 = removedEdges;
        this.f4 = updatedVertices;
        this.f5 = totalVertices;
        this.f6 = totalEdges;
    }

    public long getAddedVertices() {
        return f0;
    }

    public long getRemovedVertices() {
        return f1;
    }

    public long getAddedEdges() {
        return f2;
    }

    public long getRemovedEdges() {
        return f3;
    }

    public long getUpdatedVertices() {
        return f4;
    }

    public long getTotalVertices() {
        return f5;
    }

    public long getTotalEdges() {
        return f6;
    }
}

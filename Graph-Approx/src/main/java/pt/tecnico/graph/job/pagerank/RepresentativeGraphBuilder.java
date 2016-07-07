package pt.tecnico.graph.job.pagerank;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.LongValue;
import org.apache.flink.util.Collector;

/**
 * Created by Renato on 05/07/2016.
 */
public class RepresentativeGraphBuilder<VV, EV> {

    private static final VertexKeySelector<Long> keySelector = new VertexKeySelector<>(TypeInformation.of(Long.class));
    private final Graph<Long, VV, EV> originalGraph;
    private final double initialRank;

    public RepresentativeGraphBuilder(Graph<Long, VV, EV> originalGraph, double initialRank) {
        this.originalGraph = originalGraph;
        this.initialRank = initialRank;
    }

    public Graph<Long, Double, Double> representativeGraph(DataSet<Long> updatedVertices, DataSet<Tuple2<Long, Double>> previousRanks,
                                                           int level, Vertex<Long, Double> bigVertex) throws Exception {

        DataSet<Tuple2<Long, LongValue>> outDegrees = originalGraph.outDegrees();

        // Expand the selected vertices to neighborhood given by level
        DataSet<Long> expandedVertexIds = expandedVertexIds(updatedVertices, level);

        // Generate vertices with previous rank, or initialRank (for new vertices), as values
        final double initRank = initialRank;
        DataSet<Vertex<Long, Double>> kernelVertices = expandedVertexIds
                .leftOuterJoin(previousRanks)
                .where(keySelector).equalTo(0)
                .with(new JoinFunction<Long, Tuple2<Long, Double>, Vertex<Long, Double>>() {
                    @Override
                    public Vertex<Long, Double> join(Long id, Tuple2<Long, Double> rank) throws Exception {
                        return new Vertex<>(id, rank != null ? rank.f1 : initRank);
                    }
                });

        // Select the edges between the kernel vertices, with 1/(degree of source) as the value
        DataSet<Edge<Long, Double>> internalEdges = selectEdges(kernelVertices)
                .join(outDegrees)
                .where(0).equalTo(0)
                .with(new JoinFunction<Edge<Long, Double>, Tuple2<Long, LongValue>, Edge<Long, Double>>() {
                    @Override
                    public Edge<Long, Double> join(Edge<Long, Double> edge, Tuple2<Long, LongValue> degree) throws Exception {
                        assert degree.f1.getValue() > 0;
                        edge.setValue(1.0 / degree.f1.getValue());
                        return edge;
                    }
                });

        // Select all the other edges, converted to the correct type
        DataSet<Edge<Long, Double>> externalEdges = externalEdges(internalEdges);

        // Calculate the ranks to be sent by the big vertex.
        DataSet<Tuple2<Long, Double>> ranksToSend = previousRanks.join(outDegrees)
                .where(0).equalTo(0)
                .with(new JoinFunction<Tuple2<Long, Double>, Tuple2<Long, LongValue>, Tuple2<Long, Double>>() {
                    @Override
                    public Tuple2<Long, Double> join(Tuple2<Long, Double> rank, Tuple2<Long, LongValue> degree) throws Exception {
                        if (degree.f1.getValue() > 0) {
                            return Tuple2.of(rank.f0, rank.f1 / degree.f1.getValue());
                        }
                        return Tuple2.of(rank.f0, 0.0);
                    }
                });

        // For each edge, the rank sent is (rank of original vertex)/(out degree of original vertex)
        DataSet<Edge<Long, Double>> edgesToInside = externalEdges
                .join(kernelVertices)
                .where(1).equalTo(0)
                .with(new JoinFunction<Edge<Long, Double>, Vertex<Long, Double>, Edge<Long, Double>>() {
                    @Override
                    public Edge<Long, Double> join(Edge<Long, Double> e, Vertex<Long, Double> v) throws Exception {
                        return e;
                    }
                })
                .join(ranksToSend)
                .where(0).equalTo(0)
                .with(new JoinFunction<Edge<Long, Double>, Tuple2<Long, Double>, Edge<Long, Double>>() {
                    @Override
                    public Edge<Long, Double> join(Edge<Long, Double> edge, Tuple2<Long, Double> rank) throws Exception {
                        edge.setSource(bigVertex.getId());
                        edge.setValue(rank.f1);
                        return edge;
                    }
                })
                .groupBy(0, 1)
                .aggregate(Aggregations.SUM, 2);

        // Add the big vertex to the set
        DataSet<Vertex<Long, Double>> vertices = kernelVertices.union(originalGraph.getContext().fromElements(bigVertex));

        // Build the edge set
        DataSet<Edge<Long, Double>> edges = internalEdges.union(edgesToInside);

        return Graph.fromDataSet(vertices, edges, originalGraph.getContext());
    }

    public DataSet<Long> expandedVertexIds(DataSet<Long> originalVertexIds, int level) throws Exception {
        DataSet<Long> expandedIds = originalVertexIds;
        VertexKeySelector<Long> keySelector = new VertexKeySelector<>(TypeInformation.of(Long.class));
        while (level > 0) {
            DataSet<Long> firstNeighbours = expandedIds
                    .join(originalGraph.getEdges())
                    .where(keySelector).equalTo(0)
                    .with((id, e) -> e.getTarget())
                    .returns(expandedIds.getType());

            expandedIds = expandedIds.union(firstNeighbours).distinct();
            level--;
        }

        return expandedIds;
    }

    public DataSet<Edge<Long, Double>> selectEdges(DataSet<Vertex<Long, Double>> vertices) {
        return vertices
                .joinWithHuge(originalGraph.getEdges())
                .where(0).equalTo(0)
                .with(new JoinFunction<Vertex<Long, Double>, Edge<Long, EV>, Edge<Long, EV>>() {
                    @Override
                    public Edge<Long, EV> join(Vertex<Long, Double> source, Edge<Long, EV> edge) throws Exception {
                        return edge;
                    }
                })
                .join(vertices)
                .where(1).equalTo(0)
                .with(new JoinFunction<Edge<Long, EV>, Vertex<Long, Double>, Edge<Long, Double>>() {
                    @Override
                    public Edge<Long, Double> join(Edge<Long, EV> e, Vertex<Long, Double> v) throws Exception {
                        return new Edge<Long, Double>(e.getSource(), e.getTarget(), 0.0);
                    }
                })
                .distinct(0, 1);
    }

    public DataSet<Edge<Long, Double>> externalEdges(DataSet<Edge<Long, Double>> edgesToBeRemoved) {
        return originalGraph.getEdges().coGroup(edgesToBeRemoved)
                .where(0, 1).equalTo(0, 1)
                .with(new CoGroupFunction<Edge<Long, EV>, Edge<Long, Double>, Edge<Long, Double>>() {
                    @Override
                    public void coGroup(Iterable<Edge<Long, EV>> edge, Iterable<Edge<Long, Double>> edgeToBeRemoved, Collector<Edge<Long, Double>> out) throws Exception {
                        if (!edgeToBeRemoved.iterator().hasNext()) {
                            for (Edge<Long, EV> next : edge) {
                                out.collect(new Edge<>(next.getSource(), next.getTarget(), 0.0));
                            }
                        }
                    }
                });
    }

    private static class VertexKeySelector<K> implements KeySelector<K, K>, ResultTypeQueryable<K> {
        TypeInformation<K> type;

        public VertexKeySelector(TypeInformation<K> type) {
            this.type = type;
        }

        @Override
        public K getKey(K value) throws Exception {
            return value;
        }

        @Override
        public TypeInformation<K> getProducedType() {
            return type;
        }
    }

}

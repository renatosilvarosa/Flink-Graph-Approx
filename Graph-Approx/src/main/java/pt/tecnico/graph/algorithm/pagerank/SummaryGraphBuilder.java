package pt.tecnico.graph.algorithm.pagerank;

import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.RichJoinFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.LongValue;
import org.apache.flink.util.Collector;

/**
 * Created by Renato on 05/07/2016.
 */
public class SummaryGraphBuilder<K, VV, EV> {

    private static final VertexKeySelector<Long> keySelector = new VertexKeySelector<>(TypeInformation.of(Long.class));
    private static final TypeInformation<Edge<Long, Double>> edgeTypeInfo = TypeInformation.of(new TypeHint<Edge<Long, Double>>() {
    });
    private static final TypeInformation<Tuple2<Long, Double>> tuple2TypeInfo = TypeInformation.of(new TypeHint<Tuple2<Long, Double>>() {
    });
    private final Graph<Long, VV, EV> originalGraph;
    private final double initialRank;

    public SummaryGraphBuilder(Graph<Long, VV, EV> originalGraph, double initialRank) {
        this.originalGraph = originalGraph;
        this.initialRank = initialRank;
    }

    public Graph<Long, Double, Double> representativeGraph(DataSet<Long> updatedVertices, DataSet<Tuple2<Long, Double>> previousRanks,
                                                           int level, Vertex<Long, Double> bigVertex) throws Exception {

        DataSet<Tuple2<Long, LongValue>> outDegrees = originalGraph.outDegrees();

        // Expand the selected vertices to neighborhood given by level
        DataSet<Long> expandedVertexIds = expandedVertexIds(updatedVertices, level);

        // Generate vertices with previous rank, or initialRank (for new vertices), as values
        DataSet<Vertex<Long, Double>> kernelVertices = expandedVertexIds
                .leftOuterJoin(previousRanks)
                .where(keySelector).equalTo(0)
                .with(new KernelVertexJoinFunction(initialRank));

        // Select the edges between the kernel vertices, with 1/(degree of source) as the value
        DataSet<Edge<Long, Double>> internalEdges = selectEdges(kernelVertices)
                .join(outDegrees)
                .where(0).equalTo(0)
                .with((edge, degree) -> {
                    assert degree.f1.getValue() > 0;
                    edge.setValue(1.0 / degree.f1.getValue());
                    return edge;
                }).returns(edgeTypeInfo)
                .withForwardedFieldsFirst("f0;f1");

        // Select all the other edges, converted to the correct type
        DataSet<Edge<Long, Double>> externalEdges = externalEdges(internalEdges);

        // Calculate the ranks to be sent by the big vertex.
        DataSet<Tuple2<Long, Double>> ranksToSend = previousRanks.join(outDegrees)
                .where(0).equalTo(0)
                .with((rank, degree) -> degree.f1.getValue() > 0 ?
                        Tuple2.of(rank.f0, rank.f1 / degree.f1.getValue()) : Tuple2.of(rank.f0, 0.0))
                .returns(tuple2TypeInfo)
                .withForwardedFieldsFirst("f0");

        // For each edge, the rank sent is (rank of original vertex)/(out degree of original vertex)
        DataSet<Edge<Long, Double>> edgesToInside = externalEdges
                .join(kernelVertices)
                .where(1).equalTo(0)
                .with((e, v) -> e)
                .returns(edgeTypeInfo)
                .withForwardedFieldsFirst("*->*")
                .join(ranksToSend)
                .where(0).equalTo(0)
                .with((edge, rank) -> {
                    edge.setSource(bigVertex.getId());
                    edge.setValue(rank.f1);
                    return edge;
                }).returns(edgeTypeInfo)
                .withForwardedFieldsFirst("f1")
                .withForwardedFieldsSecond("f1->f2")
                .groupBy(0, 1)
                .aggregate(Aggregations.SUM, 2);

        // Add the big vertex to the set
        DataSet<Vertex<Long, Double>> vertices = kernelVertices.union(originalGraph.getContext().fromElements(bigVertex));

        // Build the edge set
        DataSet<Edge<Long, Double>> edges = internalEdges.union(edgesToInside);

        return Graph.fromDataSet(vertices, edges, originalGraph.getContext());
    }

    private DataSet<Long> expandedVertexIds(DataSet<Long> originalVertexIds, int level) throws Exception {
        DataSet<Long> expandedIds = originalVertexIds;
        VertexKeySelector<Long> keySelector = new VertexKeySelector<>(TypeInformation.of(Long.class));
        while (level > 0) {
            DataSet<Long> firstNeighbours = expandedIds
                    .join(originalGraph.getEdges())
                    .where(keySelector).equalTo(0)
                    .with((id, e) -> e.getTarget())
                    .returns(expandedIds.getType())
                    .withForwardedFieldsSecond("f1->*");

            expandedIds = expandedIds.union(firstNeighbours).distinct();
            level--;
        }

        return expandedIds;
    }

    private DataSet<Edge<Long, Double>> selectEdges(DataSet<Vertex<Long, Double>> vertices) {
        return vertices
                .joinWithHuge(originalGraph.getEdges())
                .where(0).equalTo(0)
                .with((source, edge) -> edge)
                .returns(originalGraph.getEdges().getType())
                .withForwardedFieldsSecond("*->*")
                .join(vertices)
                .where(1).equalTo(0)
                .with((e, v) -> new Edge<>(e.getSource(), e.getTarget(), 0.0))
                .returns(edgeTypeInfo)
                .withForwardedFieldsFirst("f0;f1")
                .distinct(0, 1);
    }

    private DataSet<Edge<Long, Double>> externalEdges(DataSet<Edge<Long, Double>> edgesToBeRemoved) {
        return originalGraph.getEdges().coGroup(edgesToBeRemoved)
                .where(0, 1).equalTo(0, 1)
                .with((Iterable<Edge<Long, EV>> edge, Iterable<Edge<Long, Double>> edgeToBeRemoved, Collector<Edge<Long, Double>> out) -> {
                    if (!edgeToBeRemoved.iterator().hasNext()) {
                        for (Edge<Long, EV> next : edge) {
                            out.collect(new Edge<>(next.getSource(), next.getTarget(), 0.0));
                        }
                    }
                }).returns(edgeTypeInfo);
    }

    @FunctionAnnotation.ForwardedFieldsFirst("*->f0")
    private static class KernelVertexJoinFunction extends RichJoinFunction<Long, Tuple2<Long, Double>, Vertex<Long, Double>> {
        final double initRank;
        private final LongCounter vertexCounter = new LongCounter();

        private KernelVertexJoinFunction(double initRank) {
            this.initRank = initRank;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            getRuntimeContext().addAccumulator("vertex-counter", vertexCounter);
        }

        @Override
        public Vertex<Long, Double> join(Long id, Tuple2<Long, Double> rank) throws Exception {
            vertexCounter.add(1);
            return new Vertex<>(id, rank != null ? rank.f1 : initRank);
        }
    }

    private static class VertexKeySelector<K> implements KeySelector<K, K>, ResultTypeQueryable<K> {
        TypeInformation<K> type;

        private VertexKeySelector(TypeInformation<K> type) {
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

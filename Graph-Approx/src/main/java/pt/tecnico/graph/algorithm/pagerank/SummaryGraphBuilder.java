package pt.tecnico.graph.algorithm.pagerank;

import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichJoinFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.LongValue;
import pt.tecnico.graph.algorithm.GraphUtils;

import java.io.Serializable;

public class SummaryGraphBuilder<VV, EV> implements Serializable {

    private static final VertexKeySelector<Long> keySelector = new VertexKeySelector<>(TypeInformation.of(Long.class));
    private static final TypeInformation<Edge<Long, Double>> edgeTypeInfo = TypeInformation.of(new TypeHint<Edge<Long, Double>>() {
    });
    private static final TypeInformation<Tuple2<Long, Double>> tuple2TypeInfo = TypeInformation.of(new TypeHint<Tuple2<Long, Double>>() {
    });
    private transient final Graph<Long, VV, EV> originalGraph;
    private final double initialRank;

    public SummaryGraphBuilder(Graph<Long, VV, EV> originalGraph, double initialRank) {
        this.originalGraph = originalGraph;
        this.initialRank = initialRank;
    }

    public Graph<Long, Double, Double> summaryGraph(DataSet<Long> vertexIds, DataSet<Tuple2<Long, Double>> previousRanks,
                                                    Vertex<Long, Double> bigVertex) throws Exception {

        DataSet<Tuple2<Long, LongValue>> outDegrees = originalGraph.outDegrees();

        // Generate vertexIds with previous rank, or initialRank (for new vertexIds), as values
        DataSet<Vertex<Long, Double>> kernelVertices = vertexIds
                .leftOuterJoin(previousRanks)
                .where(keySelector).equalTo(0)
                .with(new KernelVertexJoinFunction(initialRank))
                .name("Kernel vertices");

        Graph<Long, Double, Double> doubleGraph = originalGraph
                .mapVertices(new MapFunction<Vertex<Long, VV>, Double>() {
                    @Override
                    public Double map(Vertex<Long, VV> longVVVertex) throws Exception {
                        return 0.0;
                    }
                })
                .mapEdges(new MapFunction<Edge<Long, EV>, Double>() {
                    @Override
                    public Double map(Edge<Long, EV> longEVEdge) throws Exception {
                        return 0.0;
                    }
                });

        // Select the edges between the kernel vertexIds, with 1/(degree of source) as the value
        DataSet<Edge<Long, Double>> internalEdges = GraphUtils.selectEdges(doubleGraph, kernelVertices)
                .join(outDegrees)
                .where(0).equalTo(0)
                .with((edge, degree) -> {
                    assert degree.f1.getValue() > 0; //since there is an edge, out degree of edge source must be at least 1
                    edge.setValue(1.0 / degree.f1.getValue());
                    return edge;
                }).returns(edgeTypeInfo);

        // Select all the other edges, converted to the correct type
        DataSet<Edge<Long, Double>> externalEdges = GraphUtils.externalEdges(doubleGraph, internalEdges);

        // Calculate the ranks to be sent by the big vertex.
        DataSet<Tuple2<Long, Double>> ranksToSend = previousRanks.join(outDegrees)
                .where(0).equalTo(0)
                .with((rank, degree) -> degree.f1.getValue() > 0 ?
                        Tuple2.of(rank.f0, rank.f1 / degree.f1.getValue()) : Tuple2.of(rank.f0, 0.0))
                .returns(tuple2TypeInfo);

        // For each edge, the rank sent is (rank of original vertex)/(out degree of original vertex)
        DataSet<Edge<Long, Double>> edgesToInside = externalEdges
                .join(kernelVertices)
                .where(1).equalTo(0)
                .with((e, v) -> e)
                .returns(edgeTypeInfo)
                .join(ranksToSend)
                .where(0).equalTo(0)
                .with((edge, rank) -> {
                    edge.setSource(bigVertex.getId());
                    edge.setValue(rank.f1);
                    return edge;
                }).returns(edgeTypeInfo)
                .groupBy(0, 1)
                .aggregate(Aggregations.SUM, 2);

        // Add the big vertex to the set
        DataSet<Vertex<Long, Double>> vertices = kernelVertices.union(originalGraph.getContext().fromElements(bigVertex));

        // Build the edge set
        DataSet<Edge<Long, Double>> edges = internalEdges.union(edgesToInside);

        return Graph.fromDataSet(vertices, edges, originalGraph.getContext());
    }

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

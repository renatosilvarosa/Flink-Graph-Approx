import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.EdgeDirection;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.util.Collector;

/**
 * Created by Renato on 12/04/2016.
 */
public class GraphSampler<K, VV, EV> {

    private final Graph<K, VV, EV> graph;

    public GraphSampler(Graph<K, VV, EV> graph) {
        this.graph = graph;
    }

    public Graph<K, VV, EV> sampleByVertices(double fraction) {
        DataSet<Vertex<K, VV>> vertexSample = DataSetUtils.sample(graph.getVertices(), false, fraction);
        DataSet<Edge<K, EV>> edgesOnSource = edgesOnSource(graph.getEdges(), vertexSample);
        DataSet<Edge<K, EV>> edges = edgesOnTarget(edgesOnSource, vertexSample);
        return Graph.fromDataSet(vertexSample, edges, graph.getContext());
    }

    public Graph<K, VV, EV> sampleByVerticesWithNeighbourhood(double fraction, EdgeDirection direction) {
        DataSet<Vertex<K, VV>> initialVertexSample = DataSetUtils.sample(graph.getVertices(), false, fraction);

        DataSet<Edge<K, EV>> edges = null;
        switch (direction) {
            case IN:
                edges = edgesOnSource(graph.getEdges(), initialVertexSample);
                break;
            case OUT:
                edges = edgesOnTarget(graph.getEdges(), initialVertexSample);
                break;
            case ALL:
                edges = edgesOnSource(graph.getEdges(), initialVertexSample)
                        .union(edgesOnTarget(graph.getEdges(), initialVertexSample))
                        .distinct(0, 1);
                break;
        }

        DataSet<Vertex<K, VV>> vertices = filterVertices(edges, graph.getVertices());
        return Graph.fromDataSet(vertices, edges, graph.getContext());
    }

    public Graph<K, VV, EV> sampleByEdges(double fraction) {
        DataSet<Edge<K, EV>> edgeSample = DataSetUtils.sample(graph.getEdges(), false, fraction);
        DataSet<Vertex<K, VV>> vertices = filterVertices(edgeSample, graph.getVertices());
        return Graph.fromDataSet(vertices, edgeSample, graph.getContext());
    }

    private DataSet<Edge<K, EV>> edgesOnSource(DataSet<Edge<K, EV>> edges, DataSet<Vertex<K, VV>> vertices) {
        return edges
                .join(vertices).where(0).equalTo(0)
                .with((edge, vertex) -> edge)
                .returns(edges.getType());
    }

    private DataSet<Edge<K, EV>> edgesOnTarget(DataSet<Edge<K, EV>> edges, DataSet<Vertex<K, VV>> vertices) {
        return edges
                .join(vertices).where(1).equalTo(0)
                .with((edge, vertex) -> edge)
                .returns(edges.getType());
    }

    private DataSet<Vertex<K, VV>> filterVertices(DataSet<Edge<K, EV>> edges, DataSet<Vertex<K, VV>> vertices) {
        DataSet<Tuple1<K>> ids = edges.flatMap(new FlatMapFunction<Edge<K, EV>, Tuple1<K>>() {
            @Override
            public void flatMap(Edge<K, EV> edge, Collector<Tuple1<K>> out) throws Exception {
                out.collect(Tuple1.of(edge.getSource()));
                out.collect(Tuple1.of(edge.getTarget()));
            }
        }).distinct(0);

        return vertices
                .join(ids).where(0).equalTo(0)
                .with((vertex, id) -> vertex)
                .returns(vertices.getType());
    }
}

package pt.tecnico.graph.algorithm;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.*;
import org.apache.flink.graph.spargel.GatherFunction;
import org.apache.flink.graph.spargel.MessageIterator;
import org.apache.flink.graph.spargel.ScatterFunction;
import org.apache.flink.graph.spargel.ScatterGatherConfiguration;
import org.apache.flink.types.LongValue;

import java.io.Serializable;

/**
 * Flink's PageRank adapted to support vertices without incoming or outgoing edges, and from any type
 *
 * @param <K>
 * @param <VV>
 * @param <EV>
 * @author Renato Rosa
 */
public class SimplePageRank<K, VV, EV> implements GraphAlgorithm<K, VV, EV, DataSet<Tuple2<K, Double>>>, Serializable {

    private final double beta;
    private final double initialRank;
    private final int iterations;
    private transient final DataSet<Tuple2<K, Double>> initialRanks;

    public SimplePageRank(double beta, double initialRank, int iterations) {
        this.beta = beta;
        this.initialRank = initialRank;
        this.initialRanks = null;
        this.iterations = iterations;
    }

    public SimplePageRank(double beta, DataSet<Tuple2<K, Double>> initialRanks, int iterations) {
        this.beta = beta;
        this.initialRank = 1.0;
        this.initialRanks = initialRanks;
        this.iterations = iterations;
    }

    @Override
    public DataSet<Tuple2<K, Double>> run(Graph<K, VV, EV> graph) throws Exception {
        DataSet<Tuple2<K, LongValue>> vertexOutDegrees = graph.outDegrees();

        Graph<K, Double, Double> g = graph
                .mapVertices(new RankInitializer())
                .mapEdges(new EdgeInitializer())
                .joinWithEdgesOnSource(vertexOutDegrees, new InitWeights());

        if (initialRanks != null) {
            g = g.joinWithVertices(initialRanks, new RanksJoinFunction());
        }

        ScatterGatherConfiguration conf = new ScatterGatherConfiguration();
        conf.setName("Simple PageRank");
        conf.setDirection(EdgeDirection.OUT);
        return g.runScatterGatherIteration(new RankMessenger(), new VertexRankUpdater(), iterations, conf)
                .getVerticesAsTuple2();
    }

    @FunctionAnnotation.ForwardedFieldsFirst("*->*")
    private static class RanksJoinFunction implements VertexJoinFunction<Double, Double> {
        @Override
        public Double vertexJoin(Double vertexValue, Double inputValue) throws Exception {
            return inputValue;
        }
    }

    private static class InitWeights implements EdgeJoinFunction<Double, LongValue> {
        @Override
        public Double edgeJoin(Double edgeValue, LongValue inputValue) {
            return edgeValue / (double) inputValue.getValue();
        }
    }

    private class VertexRankUpdater extends GatherFunction<K, Double, Double> {
        @Override
        public void updateVertex(Vertex<K, Double> vertex, MessageIterator<Double> inMessages) throws Exception {
            double rankSum = 0.0;
            for (double msg : inMessages) {
                rankSum += msg;
            }

            // apply the dampening factor / random jump
            double newRank = (beta * rankSum) + 1 - beta;
            setNewVertexValue(newRank);
        }
    }

    private class RankMessenger extends ScatterFunction<K, Double, Double, Double> {
        @Override
        public void sendMessages(Vertex<K, Double> vertex) throws Exception {
            for (Edge<K, Double> edge : getEdges()) {
                sendMessageTo(edge.getTarget(), vertex.getValue() * edge.getValue());
            }
            // dummy message to force computation for every vertex
            sendMessageTo(vertex.getId(), 0.0);
        }
    }

    private class RankInitializer implements MapFunction<Vertex<K, VV>, Double> {
        @Override
        public Double map(Vertex<K, VV> v) throws Exception {
            return initialRank;
        }
    }

    private class EdgeInitializer implements MapFunction<Edge<K, EV>, Double> {
        @Override
        public Double map(Edge<K, EV> value) throws Exception {
            return 1.0;
        }
    }
}


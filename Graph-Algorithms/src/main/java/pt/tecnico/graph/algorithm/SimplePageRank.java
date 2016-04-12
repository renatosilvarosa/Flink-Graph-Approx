package pt.tecnico.graph.algorithm;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.*;
import org.apache.flink.graph.spargel.MessageIterator;
import org.apache.flink.graph.spargel.MessagingFunction;
import org.apache.flink.graph.spargel.ScatterGatherConfiguration;
import org.apache.flink.graph.spargel.VertexUpdateFunction;

import java.io.Serializable;

/**
 * Created by Renato on 10/04/2016.
 */
public class SimplePageRank<K, VV, EV> implements GraphAlgorithm<K, VV, EV, DataSet<Vertex<K, Double>>>, Serializable {

    private final double beta;
    private final double initialRank;
    private final int iterations;

    public SimplePageRank(double beta, double initialRank, int iterations) {
        this.beta = beta;
        this.initialRank = initialRank;
        this.iterations = iterations;
    }

    @Override
    public DataSet<Vertex<K, Double>> run(Graph<K, VV, EV> graph) throws Exception {
        DataSet<Tuple2<K, Long>> vertexOutDegrees = graph.outDegrees();

        Graph<K, Double, Double> g = graph
                .mapVertices(new RankInitializer())
                .mapEdges(new EdgeInitializer())
                .joinWithEdgesOnSource(vertexOutDegrees, new InitWeights());

        ScatterGatherConfiguration conf = new ScatterGatherConfiguration();
        conf.setName("Simple PageRank");
        conf.setDirection(EdgeDirection.OUT);
        return g.runScatterGatherIteration(new VertexRankUpdater(), new RankMessenger(), iterations, conf).getVertices();
    }

    private class VertexRankUpdater extends VertexUpdateFunction<K, Double, Double> {
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

    private class RankMessenger extends MessagingFunction<K, Double, Double, Double> {
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

    private class InitWeights implements EdgeJoinFunction<Double, Long> {
        @Override
        public Double edgeJoin(Double edgeValue, Long inputValue) {
            return edgeValue / (double) inputValue;
        }
    }
}


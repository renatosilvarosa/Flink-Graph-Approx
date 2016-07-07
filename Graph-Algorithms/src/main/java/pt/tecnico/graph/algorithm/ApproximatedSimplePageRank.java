package pt.tecnico.graph.algorithm;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.*;
import org.apache.flink.graph.spargel.GatherFunction;
import org.apache.flink.graph.spargel.MessageIterator;
import org.apache.flink.graph.spargel.ScatterFunction;
import org.apache.flink.graph.spargel.ScatterGatherConfiguration;

import java.io.Serializable;

/**
 * Created by Renato on 10/04/2016.
 */
public class ApproximatedSimplePageRank implements GraphAlgorithm<Long, Double, Double, DataSet<Tuple2<Long, Double>>>, Serializable {

    private final double beta;
    private final int iterations;
    private final Long bigVertexId;

    public ApproximatedSimplePageRank(double beta, int iterations, Long bigVertexId) {
        this.beta = beta;
        this.bigVertexId = bigVertexId;
        this.iterations = iterations;
    }

    @Override
    public DataSet<Tuple2<Long, Double>> run(Graph<Long, Double, Double> graph) throws Exception {
        ScatterGatherConfiguration conf = new ScatterGatherConfiguration();
        conf.setName("Simple Approximated PageRank");
        conf.setDirection(EdgeDirection.OUT);
        return graph.runScatterGatherIteration(new RankMessenger(), new VertexRankUpdater(), iterations, conf).getVerticesAsTuple2();
    }

    private class VertexRankUpdater extends GatherFunction<Long, Double, Double> {
        @Override
        public void updateVertex(Vertex<Long, Double> vertex, MessageIterator<Double> inMessages) throws Exception {
            if (vertex.getId().equals(bigVertexId)) {
                // do not change the rank of the big vertex
                setNewVertexValue(vertex.getValue());
                return;
            }

            double rankSum = 0.0;
            for (double msg : inMessages) {
                rankSum += msg;
            }

            // apply the dampening factor / random jump
            double newRank = (beta * rankSum) + 1 - beta;
            setNewVertexValue(newRank);
        }
    }

    private class RankMessenger extends ScatterFunction<Long, Double, Double, Double> {
        @Override
        public void sendMessages(Vertex<Long, Double> vertex) throws Exception {
            if (vertex.getId().equals(bigVertexId)) {
                for (Edge<Long, Double> edge : getEdges()) {
                    sendMessageTo(edge.getTarget(), edge.getValue());
                }
            } else {
                for (Edge<Long, Double> edge : getEdges()) {
                    sendMessageTo(edge.getTarget(), vertex.getValue() * edge.getValue());
                }
            }

            // dummy message to force computation for every vertex
            sendMessageTo(vertex.getId(), 0.0);
        }
    }
}


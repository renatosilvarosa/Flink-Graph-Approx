package pt.tecnico.graph.generator;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.generator.AbstractGraphGenerator;
import org.apache.flink.graph.generator.GraphGeneratorUtils;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;
import org.apache.flink.util.LongValueSequenceIterator;

import java.io.Serializable;
import java.util.Random;

public class ErdosRenyiGraph extends AbstractGraphGenerator<LongValue, NullValue, NullValue> implements Serializable {

    // Required to create the DataSource
    private transient final ExecutionEnvironment env;
    // Required configuration
    private final long vertexCount;
    // Required configuration
    private final double edgeProbability;

    public ErdosRenyiGraph(ExecutionEnvironment env, long vertexCount, double edgeProbability) {
        if (vertexCount <= 0) {
            throw new IllegalArgumentException("Vertex count must be greater than zero");
        }

        if (edgeProbability < 0 || edgeProbability > 1) {
            throw new IllegalArgumentException("Edge probability must be between 0 and 1");
        }

        this.env = env;
        this.vertexCount = vertexCount;
        this.edgeProbability = edgeProbability;
    }

    @Override
    public Graph<LongValue, NullValue, NullValue> generate() {
        // Vertices
        DataSet<Vertex<LongValue, NullValue>> vertices = GraphGeneratorUtils.vertexSequence(env, parallelism, vertexCount);
        // Edges
        LongValueSequenceIterator iterator = new LongValueSequenceIterator(0, this.vertexCount - 1);

        DataSet<Edge<LongValue, NullValue>> edges = env
                .fromParallelCollection(iterator, LongValue.class)
                .setParallelism(parallelism)
                .name("Edge iterators")
                .flatMap(new LinkVertexWithProbability())
                .setParallelism(parallelism)
                .name("Complete graph edges");

        // Graph
        return Graph.fromDataSet(vertices, edges, env);
    }

    @ForwardedFields("*->f0")
    private class LinkVertexWithProbability implements FlatMapFunction<LongValue, Edge<LongValue, NullValue>> {
        private Random random = new Random();
        private LongValue target = new LongValue();
        private Edge<LongValue, NullValue> edge = new Edge<>(null, target, NullValue.getInstance());

        @Override
        public void flatMap(LongValue source, Collector<Edge<LongValue, NullValue>> out) throws Exception {
            edge.setSource(source);
            long s = source.getValue();
            for (int t = 0; t < vertexCount; t++) {
                if (s != t) {
                    if (random.nextDouble() <= edgeProbability) {
                        target.setValue(t);
                        out.collect(edge);
                    }
                }
            }
        }
    }
}

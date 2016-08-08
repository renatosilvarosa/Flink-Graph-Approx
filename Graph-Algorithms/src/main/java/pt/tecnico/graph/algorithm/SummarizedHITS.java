package pt.tecnico.graph.algorithm;

import org.apache.flink.api.common.aggregators.ConvergenceCriterion;
import org.apache.flink.api.common.aggregators.DoubleSumAggregator;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.operators.base.JoinOperatorBase;
import org.apache.flink.api.common.operators.base.ReduceOperatorBase;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.library.link_analysis.HITS.Result;
import org.apache.flink.graph.utils.proxy.GraphAlgorithmDelegatingDataSet;
import org.apache.flink.types.DoubleValue;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import java.util.Collection;
import java.util.Iterator;

import static org.apache.flink.api.common.ExecutionConfig.PARALLELISM_DEFAULT;

/**
 * Created by Renato on 29/07/2016.
 */
public class SummarizedHITS<K, VV, EV>
        extends GraphAlgorithmDelegatingDataSet<K, VV, EV, Result<K>> {

    private static final String CHANGE_IN_SCORES = "change in scores";

    private static final String HUBBINESS_SUM_SQUARED = "hubbiness sum squared";

    private static final String AUTHORITY_SUM_SQUARED = "authority sum squared";
    // Required configuration
    private int maxIterations;
    private double convergenceThreshold;
    // Optional configuration
    private int parallelism = PARALLELISM_DEFAULT;

    private DataSet<K> verticesToCompute;
    private DataSet<Result<K>> previousResults;

    /**
     * Hyperlink-Induced Topic Search with a fixed number of iterations.
     *
     * @param iterations fixed number of iterations
     */
    public SummarizedHITS(int iterations) {
        this(iterations, Double.MAX_VALUE);
    }

    /**
     * Hyperlink-Induced Topic Search with a convergence threshold. The algorithm
     * terminates When the total change in hub and authority scores over all
     * vertices falls to or below the given threshold value.
     *
     * @param convergenceThreshold convergence threshold for sum of scores
     */
    public SummarizedHITS(double convergenceThreshold) {
        this(Integer.MAX_VALUE, convergenceThreshold);
    }

    /**
     * Hyperlink-Induced Topic Search with a convergence threshold and a maximum
     * iteration count. The algorithm terminates after either the given number
     * of iterations or when the total change in hub and authority scores over all
     * vertices falls to or below the given threshold value.
     *
     * @param maxIterations        maximum number of iterations
     * @param convergenceThreshold convergence threshold for sum of scores
     */
    public SummarizedHITS(int maxIterations, double convergenceThreshold) {
        Preconditions.checkArgument(maxIterations > 0, "Number of iterations must be greater than zero");
        Preconditions.checkArgument(convergenceThreshold > 0.0, "Convergence threshold must be greater than zero");

        this.maxIterations = maxIterations;
        this.convergenceThreshold = convergenceThreshold;
    }

    /**
     * Override the operator parallelism.
     *
     * @param parallelism operator parallelism
     * @return this
     */
    public SummarizedHITS<K, VV, EV> setParallelism(int parallelism) {
        this.parallelism = parallelism;

        return this;
    }

    public SummarizedHITS<K, VV, EV> setVerticesToCompute(DataSet<K> verticesToCompute) {
        this.verticesToCompute = verticesToCompute;
        return this;
    }

    public SummarizedHITS<K, VV, EV> setPreviousResults(DataSet<Result<K>> previousResults) {
        this.previousResults = previousResults;
        return this;
    }

    @Override
    protected String getAlgorithmName() {
        return this.getClass().getName();
    }

    @Override
    protected boolean mergeConfiguration(GraphAlgorithmDelegatingDataSet other) {
        Preconditions.checkNotNull(other);

        if (!SummarizedHITS.class.isAssignableFrom(other.getClass())) {
            return false;
        }

        SummarizedHITS rhs = (SummarizedHITS) other;

        // merge configurations

        maxIterations = Math.max(maxIterations, rhs.maxIterations);
        convergenceThreshold = Math.min(convergenceThreshold, rhs.convergenceThreshold);
        parallelism = Math.min(parallelism, rhs.parallelism);

        return true;
    }


    @Override
    public DataSet<Result<K>> runInternal(Graph<K, VV, EV> input)
            throws Exception {

        GraphUtils.EdgeToTuple2<K, EV> mapper = new GraphUtils.EdgeToTuple2<>();
        GraphUtils.VertexKeySelector<K> keySelector = new GraphUtils.VertexKeySelector<>(input.getVertexIds().getType());

        DataSet<Edge<K, EV>> edgeDataSet = GraphUtils.selectEdgesWithIds(input, verticesToCompute);
        DataSet<Tuple2<K, K>> edges = edgeDataSet.map(mapper);

        DataSet<Tuple2<K, K>> outside = GraphUtils.externalEdges(input, edgeDataSet).map(mapper);

        DataSet<Result<K>> sumToOutside = input.getVertexIds().join(outside)
                .where(keySelector).equalTo(0)
                .with(new JoinFunction<K, Tuple2<K, K>, Tuple2<K, K>>() {
                    @Override
                    public Tuple2<K, K> join(K first, Tuple2<K, K> second) throws Exception {
                        return second;
                    }
                }).join(previousResults)
                .where(1).equalTo(0)
                .with(new JoinFunction<Tuple2<K, K>, Result<K>, Result<K>>() {
                    @Override
                    public Result<K> join(Tuple2<K, K> first, Result<K> second) throws Exception {
                        return second;
                    }
                }).groupBy(0)
                .reduce(new ReduceFunction<Result<K>>() {
                    @Override
                    public Result<K> reduce(Result<K> value1, Result<K> value2) throws Exception {
                        value1.f1.f0.setValue(value1.getHubScore().getValue() + value2.getHubScore().getValue());
                        value1.f1.f1.setValue(value1.getAuthorityScore().getValue() + value2.getAuthorityScore().getValue());
                        return value1;
                    }
                });

        DataSet<Result<K>> sumToInside = input.getVertexIds().join(outside)
                .where(keySelector).equalTo(1)
                .with(new JoinFunction<K, Tuple2<K, K>, Tuple2<K, K>>() {
                    @Override
                    public Tuple2<K, K> join(K first, Tuple2<K, K> second) throws Exception {
                        return second;
                    }
                }).join(previousResults)
                .where(0).equalTo(0)
                .with(new JoinFunction<Tuple2<K, K>, Result<K>, Result<K>>() {
                    @Override
                    public Result<K> join(Tuple2<K, K> first, Result<K> second) throws Exception {
                        return second;
                    }
                }).groupBy(1)
                .reduce(new ReduceFunction<Result<K>>() {
                    @Override
                    public Result<K> reduce(Result<K> value1, Result<K> value2) throws Exception {
                        value1.f1.f0.setValue(value1.getHubScore().getValue() + value2.getHubScore().getValue());
                        value1.f1.f1.setValue(value1.getAuthorityScore().getValue() + value2.getAuthorityScore().getValue());
                        return value1;
                    }
                });

        // ID, hub, authority
        DataSet<Tuple3<K, DoubleValue, DoubleValue>> initialScores =
                previousResults
                        .join(input.getVertexIds())
                        .where(0).equalTo(keySelector)
                        .with(new JoinFunction<Result<K>, K, Tuple3<K, DoubleValue, DoubleValue>>() {
                            @Override
                            public Tuple3<K, DoubleValue, DoubleValue> join(Result<K> first, K second) throws Exception {
                                return Tuple3.of(first.f0, first.getHubScore(), first.getAuthorityScore());
                            }
                        });

        IterativeDataSet<Tuple3<K, DoubleValue, DoubleValue>> iterative = initialScores
                .iterate(maxIterations);

        // ID, hubbiness
        DataSet<Tuple2<K, DoubleValue>> hubbiness = iterative
                .coGroup(edges)
                .where(0)
                .equalTo(1)
                .with(new Hubbiness<>())
                .setParallelism(parallelism)
                .name("Hub")
                .groupBy(0)
                .reduce(new SumScore<>())
                .setCombineHint(ReduceOperatorBase.CombineHint.HASH)
                .setParallelism(parallelism)
                .name("Sum")
                .join(sumToOutside)
                .where(0).equalTo(0)
                .with(new JoinFunction<Tuple2<K, DoubleValue>, Result<K>, Tuple2<K, DoubleValue>>() {
                    @Override
                    public Tuple2<K, DoubleValue> join(Tuple2<K, DoubleValue> first, Result<K> second) throws Exception {
                        first.f1.setValue(first.f1.getValue() + second.getAuthorityScore().getValue());
                        return first;
                    }
                });

        // sum-of-hubbiness-squared
        DataSet<DoubleValue> hubbinessSumSquared = hubbiness
                .map(new Square<>())
                .setParallelism(parallelism)
                .name("Square")
                .reduce(new Sum())
                .setCombineHint(ReduceOperatorBase.CombineHint.HASH)
                .setParallelism(parallelism)
                .name("Sum");

        // ID, new authority
        DataSet<Tuple2<K, DoubleValue>> authority = hubbiness
                .coGroup(edges)
                .where(0)
                .equalTo(0)
                .with(new Authority<K>())
                .setParallelism(parallelism)
                .name("Authority")
                .groupBy(0)
                .reduce(new SumScore<K>())
                .setCombineHint(ReduceOperatorBase.CombineHint.HASH)
                .setParallelism(parallelism)
                .name("Sum")
                .join(sumToInside)
                .where(0).equalTo(0)
                .with(new JoinFunction<Tuple2<K, DoubleValue>, Result<K>, Tuple2<K, DoubleValue>>() {
                    @Override
                    public Tuple2<K, DoubleValue> join(Tuple2<K, DoubleValue> first, Result<K> second) throws Exception {
                        first.f1.setValue(first.f1.getValue() + second.getHubScore().getValue());
                        return first;
                    }
                });

        // sum-of-authority-squared
        DataSet<DoubleValue> authoritySumSquared = authority
                .map(new Square<>())
                .setParallelism(parallelism)
                .name("Square")
                .reduce(new Sum())
                .setCombineHint(ReduceOperatorBase.CombineHint.HASH)
                .setParallelism(parallelism)
                .name("Sum");

        // ID, normalized hubbiness, normalized authority
        DataSet<Tuple3<K, DoubleValue, DoubleValue>> scores = hubbiness
                .fullOuterJoin(authority, JoinOperatorBase.JoinHint.REPARTITION_SORT_MERGE)
                .where(0)
                .equalTo(0)
                .with(new JoinAndNormalizeHubAndAuthority<>())
                .withBroadcastSet(hubbinessSumSquared, HUBBINESS_SUM_SQUARED)
                .withBroadcastSet(authoritySumSquared, AUTHORITY_SUM_SQUARED)
                .setParallelism(parallelism)
                .name("Join scores");

        DataSet<Tuple3<K, DoubleValue, DoubleValue>> passThrough;

        if (convergenceThreshold < Double.MAX_VALUE) {
            passThrough = iterative
                    .fullOuterJoin(scores, JoinOperatorBase.JoinHint.REPARTITION_SORT_MERGE)
                    .where(0)
                    .equalTo(0)
                    .with(new ChangeInScores<K>())
                    .setParallelism(parallelism)
                    .name("Change in scores");

            iterative.registerAggregationConvergenceCriterion(CHANGE_IN_SCORES, new DoubleSumAggregator(), new ScoreConvergence(convergenceThreshold));
        } else {
            passThrough = scores;
        }

        return iterative
                .closeWith(passThrough)
                .map(new TranslateResult<K>())
                .setParallelism(parallelism)
                .name("Map result");
    }

    /**
     * Map edges and remove the edge value.
     *
     * @param <T>  ID type
     * @param <ET> edge value type
     * @see Graph.ExtractEdgeIDsMapper
     */
    @FunctionAnnotation.ForwardedFields("0; 1")
    private static class ExtractEdgeIDs<T, ET>
            implements FlatMapFunction<Edge<T, ET>, Tuple2<T, T>> {
        private Tuple2<T, T> output = new Tuple2<>();

        @Override
        public void flatMap(Edge<T, ET> value, Collector<Tuple2<T, T>> out)
                throws Exception {
            output.f0 = value.f0;
            output.f1 = value.f1;
            out.collect(output);
        }
    }

    /**
     * Initialize vertices' authority scores by assigning each vertex with an
     * initial hub score of 1.0. The hub scores are initialized to zero since
     * these will be computed based on the initial authority scores.
     * <p>
     * The initial scores are non-normalized.
     *
     * @param <T> ID type
     */
    @FunctionAnnotation.ForwardedFields("1->0")
    private static class InitializeScores<T>
            implements MapFunction<Tuple2<T, T>, Tuple3<T, DoubleValue, DoubleValue>> {
        private Tuple3<T, DoubleValue, DoubleValue> output = new Tuple3<>(null, new DoubleValue(0.0), new DoubleValue(1.0));

        @Override
        public Tuple3<T, DoubleValue, DoubleValue> map(Tuple2<T, T> value) throws Exception {
            output.f0 = value.f1;
            return output;
        }
    }

    /**
     * Sum vertices' hub and authority scores.
     *
     * @param <T> ID type
     */
    @FunctionAnnotation.ForwardedFieldsFirst("0")
    @FunctionAnnotation.ForwardedFieldsSecond("0")
    private static class SumScores<T>
            implements ReduceFunction<Tuple3<T, DoubleValue, DoubleValue>> {
        @Override
        public Tuple3<T, DoubleValue, DoubleValue> reduce(Tuple3<T, DoubleValue, DoubleValue> left, Tuple3<T, DoubleValue, DoubleValue> right)
                throws Exception {
            left.f1.setValue(left.f1.getValue() + right.f1.getValue());
            left.f2.setValue(left.f2.getValue() + right.f2.getValue());
            return left;
        }
    }

    /**
     * The hub score is the sum of authority scores of vertices on out-edges.
     *
     * @param <T> ID type
     */
    @FunctionAnnotation.ForwardedFieldsFirst("2->1")
    @FunctionAnnotation.ForwardedFieldsSecond("0")
    private static class Hubbiness<T>
            implements CoGroupFunction<Tuple3<T, DoubleValue, DoubleValue>, Tuple2<T, T>, Tuple2<T, DoubleValue>> {
        private Tuple2<T, DoubleValue> output = new Tuple2<>();

        @Override
        public void coGroup(Iterable<Tuple3<T, DoubleValue, DoubleValue>> vertex, Iterable<Tuple2<T, T>> edges, Collector<Tuple2<T, DoubleValue>> out)
                throws Exception {
            Iterator<Tuple3<T, DoubleValue, DoubleValue>> iterator = vertex.iterator();
            if (iterator.hasNext()) {
                output.f1 = iterator.next().f2;

                for (Tuple2<T, T> edge : edges) {
                    output.f0 = edge.f0;
                    out.collect(output);
                }
            }
        }
    }

    /**
     * Sum vertices' scores.
     *
     * @param <T> ID type
     */
    @FunctionAnnotation.ForwardedFieldsFirst("0")
    @FunctionAnnotation.ForwardedFieldsSecond("0")
    private static class SumScore<T>
            implements ReduceFunction<Tuple2<T, DoubleValue>> {
        @Override
        public Tuple2<T, DoubleValue> reduce(Tuple2<T, DoubleValue> left, Tuple2<T, DoubleValue> right)
                throws Exception {
            left.f1.setValue(left.f1.getValue() + right.f1.getValue());
            return left;
        }
    }

    /**
     * The authority score is the sum of hub scores of vertices on in-edges.
     *
     * @param <T> ID type
     */
    @FunctionAnnotation.ForwardedFieldsFirst("1")
    @FunctionAnnotation.ForwardedFieldsSecond("1->0")
    private static class Authority<T>
            implements CoGroupFunction<Tuple2<T, DoubleValue>, Tuple2<T, T>, Tuple2<T, DoubleValue>> {
        private Tuple2<T, DoubleValue> output = new Tuple2<>();

        @Override
        public void coGroup(Iterable<Tuple2<T, DoubleValue>> vertex, Iterable<Tuple2<T, T>> edges, Collector<Tuple2<T, DoubleValue>> out)
                throws Exception {
            Iterator<Tuple2<T, DoubleValue>> iterator = vertex.iterator();
            if (iterator.hasNext()) {
                output.f1 = iterator.next().f1;

                for (Tuple2<T, T> edge : edges) {
                    output.f0 = edge.f1;
                    out.collect(output);
                }
            }
        }
    }

    /**
     * Compute the square of each score.
     *
     * @param <T> ID type
     */
    private static class Square<T>
            implements MapFunction<Tuple2<T, DoubleValue>, DoubleValue> {
        private DoubleValue output = new DoubleValue();

        @Override
        public DoubleValue map(Tuple2<T, DoubleValue> value)
                throws Exception {
            double val = value.f1.getValue();
            output.setValue(val * val);
            System.err.println(val);
            return output;
        }
    }

    /**
     * Sum over values. This specialized function is used in place of generic aggregation.
     */
    private static class Sum
            implements ReduceFunction<DoubleValue> {
        @Override
        public DoubleValue reduce(DoubleValue first, DoubleValue second)
                throws Exception {
            first.setValue(first.getValue() + second.getValue());
            return first;
        }
    }

    /**
     * Join and normalize the hub and authority scores.
     *
     * @param <T> ID type
     */
    @FunctionAnnotation.ForwardedFieldsFirst("0")
    @FunctionAnnotation.ForwardedFieldsSecond("0")
    private static class JoinAndNormalizeHubAndAuthority<T>
            extends RichJoinFunction<Tuple2<T, DoubleValue>, Tuple2<T, DoubleValue>, Tuple3<T, DoubleValue, DoubleValue>> {
        private Tuple3<T, DoubleValue, DoubleValue> output = new Tuple3<>(null, new DoubleValue(), new DoubleValue());

        private double hubbinessRootSumSquared;

        private double authorityRootSumSquared;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);

            Collection<DoubleValue> var;
            var = getRuntimeContext().getBroadcastVariable(HUBBINESS_SUM_SQUARED);
            Iterator<DoubleValue> iterator = var.iterator();
            hubbinessRootSumSquared = iterator.hasNext() ? Math.sqrt(iterator.next().getValue()) : 1.0;

            var = getRuntimeContext().getBroadcastVariable(AUTHORITY_SUM_SQUARED);
            iterator = var.iterator();
            authorityRootSumSquared = iterator.hasNext() ? Math.sqrt(iterator.next().getValue()) : 1.0;
        }

        @Override
        public Tuple3<T, DoubleValue, DoubleValue> join(Tuple2<T, DoubleValue> hubbiness, Tuple2<T, DoubleValue> authority)
                throws Exception {
            output.f0 = (authority == null) ? hubbiness.f0 : authority.f0;
            output.f1.setValue(hubbiness == null ? 0.0 : hubbiness.f1.getValue() / hubbinessRootSumSquared);
            output.f2.setValue(authority == null ? 0.0 : authority.f1.getValue() / authorityRootSumSquared);
            return output;
        }
    }

    /**
     * Computes the total sum of the change in hub and authority scores over
     * all vertices between iterations. A negative score is emitted after the
     * first iteration to prevent premature convergence.
     *
     * @param <T> ID type
     */
    @FunctionAnnotation.ForwardedFieldsFirst("0")
    @FunctionAnnotation.ForwardedFieldsSecond("*")
    private static class ChangeInScores<T>
            extends RichJoinFunction<Tuple3<T, DoubleValue, DoubleValue>, Tuple3<T, DoubleValue, DoubleValue>, Tuple3<T, DoubleValue, DoubleValue>> {
        private boolean isInitialSuperstep;

        private double changeInScores;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);

            isInitialSuperstep = (getIterationRuntimeContext().getSuperstepNumber() == 1);
            changeInScores = (isInitialSuperstep) ? -1.0 : 0.0;
        }

        @Override
        public void close() throws Exception {
            super.close();

            DoubleSumAggregator agg = getIterationRuntimeContext().getIterationAggregator(CHANGE_IN_SCORES);
            agg.aggregate(changeInScores);
        }

        @Override
        public Tuple3<T, DoubleValue, DoubleValue> join(Tuple3<T, DoubleValue, DoubleValue> first, Tuple3<T, DoubleValue, DoubleValue> second)
                throws Exception {
            if (!isInitialSuperstep) {
                changeInScores += Math.abs(second.f1.getValue() - first.f1.getValue());
                changeInScores += Math.abs(second.f2.getValue() - first.f2.getValue());
            }

            return second;
        }
    }

    /**
     * Monitors the total change in hub and authority scores over all vertices.
     * The iteration terminates when the change in scores compared against the
     * prior iteration falls below the given convergence threshold.
     * <p>
     * An optimization of this implementation of HITS is to leave the initial
     * scores non-normalized; therefore, the change in scores after the first
     * superstep cannot be measured and a negative value is emitted to signal
     * that the iteration should continue.
     */
    private static class ScoreConvergence
            implements ConvergenceCriterion<DoubleValue> {
        private double convergenceThreshold;

        public ScoreConvergence(double convergenceThreshold) {
            this.convergenceThreshold = convergenceThreshold;
        }

        @Override
        public boolean isConverged(int iteration, DoubleValue value) {
            double val = value.getValue();
            return (0 <= val && val <= convergenceThreshold);
        }
    }

    /**
     * Map the Tuple result to the return type.
     *
     * @param <T> ID type
     */
    @FunctionAnnotation.ForwardedFields("0")
    private static class TranslateResult<T>
            implements MapFunction<Tuple3<T, DoubleValue, DoubleValue>, Result<T>> {
        private Result<T> output = new Result<>();

        @Override
        public Result<T> map(Tuple3<T, DoubleValue, DoubleValue> value) throws Exception {
            output.f0 = value.f0;
            output.f1.f0 = value.f1;
            output.f1.f1 = value.f2;
            return output;
        }
    }
}
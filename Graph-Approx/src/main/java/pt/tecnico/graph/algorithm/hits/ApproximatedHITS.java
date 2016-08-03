package pt.tecnico.graph.algorithm.hits;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.io.TypeSerializerInputFormat;
import org.apache.flink.api.java.io.TypeSerializerOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.graph.EdgeDirection;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.library.link_analysis.HITS;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;
import pt.tecnico.graph.algorithm.SummarizedHITS;
import pt.tecnico.graph.stream.GraphStreamHandler;
import pt.tecnico.graph.stream.GraphUpdateTracker;
import pt.tecnico.graph.stream.StreamProvider;

import java.util.Iterator;
import java.util.Set;

/**
 * Created by Renato on 26/06/2016.
 */
public class ApproximatedHITS extends GraphStreamHandler<HITS.Result<Long>> {
    private ApproximatedHITSConfig config;

    private HITSQueryObserver observer;
    private TypeSerializerInputFormat<HITS.Result<Long>> resultInputFormat;
    private TypeSerializerOutputFormat<HITS.Result<Long>> resultOutputFormat;
    private TypeInformation<HITS.Result<Long>> resultTypeInfo;
    private DataSet<Long> computedVertices;

    public ApproximatedHITS(StreamProvider<String> updateStream, Graph<Long, NullValue, NullValue> graph) {
        super(updateStream, graph);
    }

    @Override
    public void init() throws Exception {
        graphUpdateTracker = new GraphUpdateTracker<>(graph);

        TypeInformation<Tuple2<Long, Long>> edgeTypeInfo = graph.getEdgeIds().getType();
        edgeInputFormat = new TypeSerializerInputFormat<>(edgeTypeInfo);

        edgeOutputFormat = new TypeSerializerOutputFormat<>();
        edgeOutputFormat.setInputType(edgeTypeInfo, env.getConfig());
        edgeOutputFormat.setWriteMode(FileSystem.WriteMode.OVERWRITE);

        edgeOutputFormat.setOutputFilePath(new Path("cache/edges" + iteration));
        graph.getEdgeIds().output(edgeOutputFormat);

        env.execute("Original graph reading");

        DataSet<HITS.Result<Long>> ranks = computeExact();

        resultTypeInfo = ranks.getType();
        resultInputFormat = new TypeSerializerInputFormat<>(resultTypeInfo);
        resultOutputFormat = new TypeSerializerOutputFormat<>();
        resultOutputFormat.setInputType(resultTypeInfo, env.getConfig());
        resultOutputFormat.setWriteMode(FileSystem.WriteMode.OVERWRITE);
        resultOutputFormat.setOutputFilePath(new Path("cache/ranks" + iteration));

        // iteration 0
        ranks.output(resultOutputFormat);
        outputResult("", ranks);
        env.execute("First HITS calculation");

        graphUpdateTracker.resetAll();
    }

    @Override
    public void run() {
        try {
            observer.onStart();
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }

        while (true) {
            try {
                String update = pendingUpdates.take();
                String[] split = update.split(" ");
                switch (split[0]) {
                    case "A": {
                        // add edge
                        registerEdgeAdd(split);
                        break;
                    }
                    case "D":
                        // delete
                        registerEdgeDelete(split);
                        break;
                    case "Q":
                        iteration++;

                        String tag = split.length > 1 ? split[1] : "";

                        resultInputFormat.setFilePath("cache/ranks" + ((iteration - 1) % 5));
                        DataSet<HITS.Result<Long>> previousResult = env.createInput(resultInputFormat, resultTypeInfo);

                        DeciderResponse response = observer.onQuery(iteration, update, graph, graphUpdateTracker);

                        resultOutputFormat.setOutputFilePath(new Path("cache/ranks" + (iteration % 5)));

                        DataSet<HITS.Result<Long>> newResult = null;

                        if (response != DeciderResponse.NO_UPDATE_AND_REPEAT_LAST_ANSWER) {
                            applyUpdates();
                        }

                        switch (response) {
                            case NO_UPDATE_AND_REPEAT_LAST_ANSWER:
                            case UPDATE_AND_REPEAT_LAST_ANSWER:
                                newResult = previousResult;
                                computedVertices = null;
                                break;
                            case COMPUTE_APPROXIMATE:
                                newResult = computeApproximate(previousResult);
                                break;
                            case COMPUTE_EXACT:
                                newResult = computeExact();
                                computedVertices = graph.getVertexIds();
                                break;
                        }

                        assert newResult != null : "Ranks are null"; //should never happen
                        outputResult(tag, newResult);
                        newResult.output(resultOutputFormat);

                        env.execute("Approx HITS it. " + iteration);
                        observer.onQueryResult(iteration, update, response, graph, computedVertices, newResult, env.getLastJobExecutionResult());

                        break;
                    case "END":
                        observer.onStop();
                        return;
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private DataSet<HITS.Result<Long>> computeExact() throws Exception {
        return graph.run(new HITS<>(config.getIterations()));
    }

    private DataSet<HITS.Result<Long>> computeApproximate(DataSet<HITS.Result<Long>> previousResults) throws Exception {
        Set<Long> updatedIds = graphUpdateTracker.updatedAboveThresholdVertexIds(config.getUpdatedRatioThreshold(), EdgeDirection.ALL);

        computedVertices = env.fromCollection(updatedIds, TypeInformation.of(Long.class));

        SummarizedHITS<Long, NullValue, NullValue> summarizedHITS = new SummarizedHITS<Long, NullValue, NullValue>(30)
                .setVerticesToCompute(computedVertices)
                .setPreviousResults(previousResults);

        DataSet<HITS.Result<Long>> ranks = graph.run(summarizedHITS);

        ranks = previousResults.coGroup(ranks)
                .where(0).equalTo(0)
                .with(new CoGroupFunction<HITS.Result<Long>, HITS.Result<Long>, HITS.Result<Long>>() {
                    @Override
                    public void coGroup(Iterable<HITS.Result<Long>> previous, Iterable<HITS.Result<Long>> current, Collector<HITS.Result<Long>> out) throws Exception {
                        Iterator<HITS.Result<Long>> prevIt = previous.iterator();
                        Iterator<HITS.Result<Long>> newIt = current.iterator();

                        if (newIt.hasNext()) {
                            out.collect(newIt.next());
                        } else if (prevIt.hasNext()) {
                            out.collect(prevIt.next());
                        }
                    }
                }).returns(new TypeHint<HITS.Result<Long>>() {
                }).withForwardedFieldsFirst("f0;f1");

        graphUpdateTracker.reset(updatedIds);
        return ranks;
    }

    private void outputResult(String date, DataSet<HITS.Result<Long>> ranks) {
        outputFormat.setIteration(iteration);
        outputFormat.setTags(date);
        ranks.sortPartition(1, Order.DESCENDING).setParallelism(1).first(config.getOutputSize()).output(outputFormat);
    }

    public ApproximatedHITSConfig getConfig() {
        return config;
    }

    public void setConfig(ApproximatedHITSConfig config) {
        this.config = config;
    }

    public ApproximatedHITS setObserver(HITSQueryObserver observer) {
        this.observer = observer;
        return this;
    }

    public enum DeciderResponse {
        NO_UPDATE_AND_REPEAT_LAST_ANSWER,
        UPDATE_AND_REPEAT_LAST_ANSWER,
        COMPUTE_APPROXIMATE,
        COMPUTE_EXACT
    }

}

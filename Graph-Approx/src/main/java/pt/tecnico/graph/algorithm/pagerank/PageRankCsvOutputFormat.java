package pt.tecnico.graph.algorithm.pagerank;

import org.apache.flink.api.java.io.CsvOutputFormat;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import pt.tecnico.graph.output.GraphOutputFormat;

import java.io.IOException;

/**
 * CSV output for PageRank
 *
 * @author Renato Rosa
 */
public class PageRankCsvOutputFormat implements GraphOutputFormat<Tuple2<Long, Double>> {
    private final Path outputDir;
    private final boolean printRanks;
    private String name;
    private int iteration;
    private String[] tags;
    private CsvOutputFormat<? extends Tuple> format;

    public PageRankCsvOutputFormat(String outputDir, String recordDelimiter, String fieldDelimiter, boolean printRanks, boolean overwrite) {
        this.outputDir = new Path(outputDir);
        this.printRanks = printRanks;
        if (printRanks) {
            format = new CsvOutputFormat<Tuple2<Long, Double>>(new Path(), recordDelimiter, fieldDelimiter);
        } else {
            format = new CsvOutputFormat<Tuple1<Long>>(new Path(), recordDelimiter, fieldDelimiter);
        }
        format.setWriteMode(overwrite ? FileSystem.WriteMode.OVERWRITE : FileSystem.WriteMode.NO_OVERWRITE);
    }

    @Override
    public void setName(String name) {
        this.name = name;
    }

    @Override
    public void setIteration(int iteration) {
        this.iteration = iteration;
    }

    @Override
    public void setTags(String... tags) {
        this.tags = tags.clone();
    }

    @Override
    public void configure(Configuration parameters) {
        format.configure(parameters);
    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        String fileName = String.format("%s-%04d-%s.csv", name, iteration, (tags.length > 0 ? tags[0] : ""));
        this.format.setOutputFilePath(new Path(outputDir, fileName));
        this.format.open(taskNumber, numTasks);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void writeRecord(Tuple2<Long, Double> record) throws IOException {
        if (printRanks) {
            ((CsvOutputFormat<Tuple2<Long, Double>>) format).writeRecord(record);
        } else {
            ((CsvOutputFormat<Tuple1<Long>>) format).writeRecord(Tuple1.of(record.f0));
        }
    }

    @Override
    public void close() throws IOException {
        format.close();
    }
}

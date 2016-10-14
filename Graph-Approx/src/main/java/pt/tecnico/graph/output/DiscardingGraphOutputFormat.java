package pt.tecnico.graph.output;

import org.apache.flink.configuration.Configuration;

import java.io.IOException;

public class DiscardingGraphOutputFormat<T> implements GraphOutputFormat<T> {
    @Override
    public void setName(String name) {

    }

    @Override
    public void setIteration(int iteration) {

    }

    @Override
    public void setTags(String... tags) {

    }

    @Override
    public void configure(Configuration parameters) {

    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {

    }

    @Override
    public void writeRecord(T record) throws IOException {

    }

    @Override
    public void close() throws IOException {

    }
}

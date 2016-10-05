package pt.tecnico.graph.output;

import org.apache.flink.api.common.io.OutputFormat;

public interface GraphOutputFormat<T> extends OutputFormat<T> {
    void setName(String name);

    void setIteration(int iteration);

    void setTags(String... tags);
}

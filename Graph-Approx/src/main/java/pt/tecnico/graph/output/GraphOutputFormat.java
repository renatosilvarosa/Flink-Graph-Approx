package pt.tecnico.graph.output;

import org.apache.flink.api.common.io.OutputFormat;

/**
 * Created by Renato on 07/07/2016.
 */
public interface GraphOutputFormat<T> extends OutputFormat<T> {
    void setName(String name);

    void setIteration(int iteration);

    void setTags(String... tags);
}

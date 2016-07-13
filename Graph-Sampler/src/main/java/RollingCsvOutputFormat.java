import org.apache.flink.api.java.io.CsvOutputFormat;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;

import java.io.IOException;

/**
 * Created by Renato on 10/04/2016.
 */
public class RollingCsvOutputFormat<T extends Tuple> extends CsvOutputFormat<T> {

    private final String basePath;
    private int iteration;

    public RollingCsvOutputFormat(String basePath) {
        super(new Path(basePath));
        this.basePath = basePath;
    }

    @Override
    public void configure(Configuration parameters) {
        super.configure(parameters);
        this.iteration = parameters.getInteger(TimedApproxGraphGob.ITERATION, 0);
    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        setOutputFilePath(new Path(String.format("%s%03d.csv", basePath, iteration)));
        super.open(taskNumber, numTasks);
    }
}

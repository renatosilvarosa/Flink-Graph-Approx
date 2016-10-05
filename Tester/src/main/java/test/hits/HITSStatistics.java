package test.hits;

import org.apache.flink.types.NullValue;
import pt.tecnico.graph.algorithm.hits.HITSQueryObserver;

import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public abstract class HITSStatistics implements HITSQueryObserver<Long, NullValue> {
    protected final String dir;
    protected final String fileName;
    protected PrintStream printStream;

    public HITSStatistics(String fileName, String dir) {
        this.fileName = fileName;
        this.dir = dir;
    }

    @Override
    public void onStart() throws Exception {
        Path dirs = Files.createDirectories(Paths.get(dir));

        Path file = dirs.resolve(fileName);//"exact-pr-time.csv"
        if (!Files.exists(file)) {
            file = Files.createFile(file);
        }
        printStream = new PrintStream(file.toString());
    }

    @Override
    public void onStop() throws Exception {
        printStream.close();
    }
}

package test.pr;

import pt.tecnico.graph.algorithm.pagerank.PageRankQueryObserver;

import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Created by Renato on 27/07/2016.
 */
public abstract class PRStatistics implements PageRankQueryObserver {
    private final String dir;
    private final String fileName;
    protected PrintStream printStream;

    public PRStatistics(String fileName, String dir) {
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

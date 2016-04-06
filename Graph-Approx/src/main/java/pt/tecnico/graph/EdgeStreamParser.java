package pt.tecnico.graph;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.types.NullValue;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Queue;

/**
 * Created by Renato on 01/04/2016.
 */
public class EdgeStreamParser {

    private final Configuration configuration;
    private final Queue<List<Edge<Integer, NullValue>>> updateQueue;
    private String separator;
    private List<Edge<Integer, NullValue>> accumulator = new ArrayList<>();

    public EdgeStreamParser(Configuration configuration, Queue<List<Edge<Integer, NullValue>>> updateQueue) {
        this.configuration = configuration;
        this.updateQueue = updateQueue;
    }

    public EdgeStreamParser withSeparator(String separator) {
        this.separator = separator;
        return this;
    }



    public void parseFromSocket(String host, int port) throws IOException {
        try (Socket s = new Socket(host, port)) {
            BufferedReader br = new BufferedReader(new InputStreamReader(s.getInputStream()));
            String line;
            while ((line = br.readLine()) != null) {
                final String[] vs = line.split(separator);
                Integer v1 = Integer.valueOf(vs[0]);
                Integer v2 = Integer.valueOf(vs[1]);
                addEdgeUpdate(v1, v2);
            }
        }
    }

    public void parseFromFile(String path) throws IOException {
        try (BufferedReader br = Files.newBufferedReader(Paths.get(path))) {
            String line;
            while ((line = br.readLine()) != null) {
                final String[] vs = line.split(separator);
                Integer v1 = Integer.valueOf(vs[0]);
                Integer v2 = Integer.valueOf(vs[1]);
                addEdgeUpdate(v1, v2);
            }
        }
    }

    public void parseFromCollection(Collection<Tuple2<Integer,Integer>> tuples) {
        for (Tuple2<Integer, Integer> t : tuples) {
            addEdgeUpdate(t.f0, t.f1);
        }
    }

    private void addEdgeUpdate(Integer v1, Integer v2) {
        Edge<Integer, NullValue> edge = new Edge<>(v1, v2, NullValue.getInstance());
        accumulator.add(edge);
        if (accumulator.size() >= configuration.getMaximumUpdates()) {
            updateQueue.add(accumulator);
            accumulator = new ArrayList<>();
        }
    }
}

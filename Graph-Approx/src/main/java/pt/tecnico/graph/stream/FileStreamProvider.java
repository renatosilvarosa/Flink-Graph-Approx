package pt.tecnico.graph.stream;

import org.apache.flink.api.common.functions.MapFunction;

import java.io.BufferedReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Stream provider for data coming from a file
 *
 * @param <V>
 * @author Renato Rosa
 */
public class FileStreamProvider<V> extends MappingStreamProvider<String, V> {

    private final Path filePath;

    public FileStreamProvider(String path, MapFunction<String, V> mapFunction) {
        super(mapFunction);
        this.filePath = Paths.get(path);
    }

    @Override
    public void run() {
        try (BufferedReader br = Files.newBufferedReader(filePath)) {
            String line;
            while ((line = br.readLine()) != null) {
                mapAndPut(line);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

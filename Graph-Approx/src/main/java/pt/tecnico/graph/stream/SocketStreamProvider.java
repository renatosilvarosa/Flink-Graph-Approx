package pt.tecnico.graph.stream;

import org.apache.flink.api.common.functions.MapFunction;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.Socket;

/**
 * Created by Renato on 01/04/2016.
 */
public class SocketStreamProvider<V> extends MappingStreamProvider<String, V> {

    private final String host;
    private final int port;

    public SocketStreamProvider(String host, int port, MapFunction<String, V> mapFunction) {
        super(mapFunction);
        this.host = host;
        this.port = port;
    }

    @Override
    public void run() {
        try (Socket s = new Socket(host, port)) {
            BufferedReader br = new BufferedReader(new InputStreamReader(s.getInputStream()));
            String line;
            while ((line = br.readLine()) != null) {
                mapAndPut(line);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

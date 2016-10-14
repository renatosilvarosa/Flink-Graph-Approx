package pt.tecnico.graph.stream;

import org.apache.flink.api.common.functions.MapFunction;

public abstract class MappingStreamProvider<T, V> extends StreamProvider<V> {

    private MapFunction<T, V> mapFunction;

    public MappingStreamProvider(MapFunction<T, V> mapFunction) {
        this.mapFunction = mapFunction;
    }

    protected void mapAndPut(T el) throws Exception {
        queue.put(mapFunction.map(el));
    }
}

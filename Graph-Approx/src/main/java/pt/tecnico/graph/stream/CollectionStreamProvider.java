package pt.tecnico.graph.stream;

import java.util.Collection;

public class CollectionStreamProvider<T> extends StreamProvider<T> {
    private final Collection<T> collection;

    public CollectionStreamProvider(Collection<T> collection) {
        this.collection = collection;
    }

    @Override
    public void run() {
        this.queue.addAll(collection);
    }
}

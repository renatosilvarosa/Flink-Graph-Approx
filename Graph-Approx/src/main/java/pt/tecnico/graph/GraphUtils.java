package pt.tecnico.graph;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.objenesis.Objenesis;
import org.objenesis.ObjenesisStd;

import java.util.Collections;

/**
 * Created by Renato on 09/04/2016.
 */
public class GraphUtils {

    private static Objenesis instantiator = new ObjenesisStd(true);

    public static <T> DataSet<T> emptyDataSet(ExecutionEnvironment env, TypeInformation<T> typeInformation) {
        return env.fromCollection(Collections.emptyList(), typeInformation);
    }

    public static <T> DataSet<T> emptyDataSet(ExecutionEnvironment env, TypeHint<T> typeHint) {
        return env.fromCollection(Collections.emptyList(), typeHint.getTypeInfo());
    }

    public static <T> DataSet<T> emptyDataSet(ExecutionEnvironment env, Class<T> clazz) {
        return env.fromCollection(Collections.emptyList(), TypeInformation.of(clazz));
    }

    public static <K, VV, EV> Graph<K, VV, EV> emptyGraph(ExecutionEnvironment env, Class<K> keyType,
                                                          Class<VV> vertexType, Class<EV> edgeType) {
        K k = instantiator.newInstance(keyType);
        VV vv = instantiator.newInstance(vertexType);
        EV ev = instantiator.newInstance(edgeType);

        TypeInformation<Vertex<K, VV>> vertexInfo = TypeExtractor.getForObject(new Vertex<>(k, vv));
        TypeInformation<Edge<K, EV>> edgeInfo = TypeExtractor.getForObject(new Edge<>(k, k, ev));

        return Graph.fromDataSet(emptyDataSet(env, vertexInfo), emptyDataSet(env, edgeInfo), env);

    }

    public static <K> Graph<K, NullValue, NullValue> emptyGraph(ExecutionEnvironment env, Class<K> keyType) {
        K k = instantiator.newInstance(keyType);

        TypeInformation<Vertex<K, NullValue>> vertexInfo = TypeExtractor.getForObject(new Vertex<>(k, NullValue.getInstance()));
        TypeInformation<Edge<K, NullValue>> edgeInfo = TypeExtractor.getForObject(new Edge<>(k, k, NullValue.getInstance()));

        return Graph.fromDataSet(emptyDataSet(env, vertexInfo), emptyDataSet(env, edgeInfo), env);

    }

}

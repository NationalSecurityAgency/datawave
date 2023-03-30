package datawave.query.function.util;

import com.esotericsoftware.kryo.Serializer;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

public class KryoDocumentOptions {
    public final static Object CACHE_KEY = new Object();
    private final Map<Class<?>,Serializer<?>> serializerMap;
    
    public KryoDocumentOptions() {
        this.serializerMap = new LinkedHashMap<>();
    }
    
    public void addSerializer(Class<?> typeClass, Serializer<?> serializer) {
        serializerMap.put(typeClass, serializer);
    }
    
    public <T> Optional<Serializer<T>> getSerializer(Class<?> typeClass) {
        Serializer<?> serializer = serializerMap.get(typeClass);
        return Optional.ofNullable(serializer != null ? (Serializer<T>) serializer : null);
    }
}

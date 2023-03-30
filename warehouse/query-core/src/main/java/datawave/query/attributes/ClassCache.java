package datawave.query.attributes;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.serializers.FieldSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class ClassCache {
    private final static Logger LOG = LoggerFactory.getLogger(Class.class);
    
    private final Map<String,Class<?>> classMap;
    private final Map<String,Serializer<?>> fieldSerializerMap;
    
    public ClassCache(int expectedSize) {
        classMap = new HashMap<>(expectedSize);
        fieldSerializerMap = new HashMap<>(expectedSize);
    }
    
    public Class<?> get(String name) {
        LOG.debug("Fetching class for {}", name);
        return classMap.computeIfAbsent(name, (className) -> {
            // Get the name of the concrete Attribute
                        Class<?> returnClz;
                        try {
                            returnClz = Class.forName(name);
                        } catch (ClassNotFoundException e) {
                            throw new RuntimeException(e);
                        }
                        
                        // TODO: follow-up type throws exception
                        // if (!Attribute.class.isAssignableFrom(returnClz)) {
                        // throw new ClassCastException("Found class that was not an instance of Attribute");
                        // }
                        return returnClz;
                    });
    }
    
    public <T> Serializer<T> getSerializer(Kryo kryo, String named, Class<T> classType) {
        return (Serializer<T>) fieldSerializerMap.computeIfAbsent(named, (serializerName) -> new FieldSerializer<>(kryo, classType));
        // return (Serializer<T>)fieldSerializerMap.computeIfAbsent(named, (serializerName) -> new JavaSerializer());
    }
}

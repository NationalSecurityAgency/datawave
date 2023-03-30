package datawave.query.attributes.serialization.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import datawave.data.type.BaseType;
import datawave.data.type.TypeBuilder;

import java.io.Serializable;
import java.util.IdentityHashMap;
import java.util.Map;

public class DelegateTypeSerializer<S extends Comparable<S> & Serializable,T extends BaseType<S>> extends Serializer<T> {
    private final Serializer<S> delegateSerializer;
    private final Class<S> delegateType;
    
    public DelegateTypeSerializer(Serializer<S> delegateSerializer, Class<S> delegateType) {
        this.delegateSerializer = delegateSerializer;
        this.delegateType = delegateType;
    }
    
    @Override
    public void write(Kryo kryo, Output output, T t) {
        DelegateTypeSerializerCache cache = getCache(kryo);
        Class<T> typeClass = (Class<T>) t.getClass();
        if (!cache.isTypeBuildable(typeClass)) {
            throw new KryoException("Encountered unexpected type, write is not supported: " + typeClass);
        }
        String normalizedValue = t.getNormalizedValue();
        output.writeString(normalizedValue);
        kryo.writeObject(output, t.getDelegate(), delegateSerializer);
    }
    
    @Override
    public T read(Kryo kryo, Input input, Class<T> typeClass) {
        DelegateTypeSerializerCache cache = getCache(kryo);
        if (!cache.isTypeBuildable(typeClass)) {
            throw new KryoException("Encountered unexpected type, read is not supported: " + typeClass);
        }
        String normalizedValue = input.readString();
        S delegateObj = kryo.readObject(input, delegateType, delegateSerializer);
        T typeObj = TypeBuilder.of(typeClass).delegate(delegateObj).normalizedValue(normalizedValue).build();
        return typeObj;
    }
    
    private DelegateTypeSerializerCache getCache(Kryo kryo) {
        DelegateTypeSerializerCache cache = (DelegateTypeSerializerCache) kryo.getContext().get(DelegateTypeSerializerCache.class);
        if (cache == null) {
            cache = new DelegateTypeSerializerCache();
            kryo.getContext().put(DelegateTypeSerializerCache.class, cache);
        }
        return cache;
    }
    
    private static class DelegateTypeSerializerCache {
        private final Map<Class<? extends BaseType<?>>,TypeBuilder> builderMap = new IdentityHashMap<>();
        private final Map<Class<? extends BaseType<?>>,TypeEntry> typeMap = new IdentityHashMap<>();
        
        private Map<Class<? extends BaseType<?>>,TypeBuilder> getBuilderMap() {
            return builderMap;
        }
        
        private boolean isTypeBuildable(Class<? extends BaseType<?>> typeClass) {
            return getTypeEntry(typeClass).supported;
        }
        
        private TypeEntry getTypeEntry(Class typeClass) {
            return typeMap.computeIfAbsent(typeClass, (key) -> {
                TypeEntry te = new TypeEntry();
                te.supported = TypeBuilder.of(typeClass).isBuildable();
                return te;
            });
        }
        
        private static class TypeEntry {
            boolean supported;
        }
    }
}

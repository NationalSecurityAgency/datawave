package datawave.query.attributes.serialization.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.FieldSerializer;
import datawave.data.type.util.IpAddress;
import datawave.data.type.util.IpV4Address;
import datawave.data.type.util.IpV6Address;

public class IpAddressDynamicSerializer extends Serializer<IpAddress> {
    @Override
    public void write(Kryo kryo, Output output, IpAddress ipAddress) {
        
    }
    
    @Override
    public IpAddress read(Kryo kryo, Input input, Class<IpAddress> aClass) {
        return null;
    }
    
    private SerializerCache getCache(Kryo kryo) {
        SerializerCache cache = (SerializerCache) kryo.getContext().get(this);
        if (cache == null) {
            cache = new SerializerCache(new FieldSerializer<>(kryo, IpV4Address.class), new FieldSerializer<>(kryo, IpV6Address.class));
            kryo.getContext().put(this, cache);
        }
        
        return cache;
    }
    
    private static class SerializerCache {
        private final FieldSerializer<IpV4Address> ipv4Serializer;
        private final FieldSerializer<IpV6Address> ipv6Serializer;
        
        SerializerCache(FieldSerializer<IpV4Address> ipv4Serializer, FieldSerializer<IpV6Address> ipv6Serializer) {
            this.ipv4Serializer = ipv4Serializer;
            this.ipv6Serializer = ipv6Serializer;
        }
        
        public Serializer<? extends IpAddress> getSerializer(Class<IpAddress> instanceClass) {
            Serializer<? extends IpAddress> serializer;
            if (instanceClass.isAssignableFrom(IpV4Address.class)) {
                serializer = ipv4Serializer;
            } else if (instanceClass.isAssignableFrom(IpV6Address.class)) {
                serializer = ipv6Serializer;
            } else {
                throw new IllegalStateException("Unexpected ip-address type: " + instanceClass);
            }
            return serializer;
        }
        
        public FieldSerializer<IpV4Address> getIpv4Serializer() {
            return ipv4Serializer;
        }
        
        public FieldSerializer<IpV6Address> getIpv6Serializer() {
            return ipv6Serializer;
        }
    }
}

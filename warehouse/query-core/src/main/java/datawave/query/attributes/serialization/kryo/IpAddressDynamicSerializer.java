package datawave.query.attributes.serialization.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Registration;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import datawave.data.type.util.IpAddress;
import datawave.data.type.util.IpV4Address;
import datawave.data.type.util.IpV6Address;

public class IpAddressDynamicSerializer extends Serializer<IpAddress> {
    @Override
    public void write(Kryo kryo, Output output, IpAddress ipAddress) {
        Serializer<? extends IpAddress> serializer = getCache(kryo).getSerializer(ipAddress.getClass());
        kryo.writeClass(output, ipAddress.getClass());
        kryo.writeObject(output, ipAddress, serializer);
    }
    
    @Override
    public IpAddress read(Kryo kryo, Input input, Class<IpAddress> aClass) {
        Registration registration = kryo.readClass(input);
        Serializer<? extends IpAddress> serializer = getCache(kryo).getSerializer(registration.getType());
        return (IpAddress) kryo.readObject(input, registration.getType(), serializer);

    }
    
    private SerializerCache getCache(Kryo kryo) {
        SerializerCache cache = (SerializerCache) kryo.getContext().get(this);
        if (cache == null) {
            cache = new SerializerCache(new IPV4AddressSerializer(), new IPV6AddressSerializer());
            kryo.getContext().put(this, cache);
        }
        
        return cache;
    }
    
    private static class SerializerCache {
        private final Serializer<IpV4Address> ipv4Serializer;
        private final Serializer<IpV6Address> ipv6Serializer;
        
        SerializerCache(Serializer<IpV4Address> ipv4Serializer, Serializer<IpV6Address> ipv6Serializer) {
            this.ipv4Serializer = ipv4Serializer;
            this.ipv6Serializer = ipv6Serializer;
        }
        
        public Serializer<? extends IpAddress> getSerializer(Class<? extends IpAddress> instanceClass) {
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
        
        public Serializer<IpV4Address> getIpv4Serializer() {
            return ipv4Serializer;
        }
        
        public Serializer<IpV6Address> getIpv6Serializer() {
            return ipv6Serializer;
        }
    }
}

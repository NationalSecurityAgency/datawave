package datawave.query.function.util;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.serializers.DefaultSerializers;
import datawave.data.type.*;
import datawave.data.type.util.Geometry;
import datawave.data.type.util.IpAddress;
import datawave.data.type.util.Point;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.serialization.kryo.DelegateTypeSerializer;
import datawave.query.attributes.serialization.kryo.GeometrySerializer;
import datawave.query.attributes.serialization.kryo.IpAddressDynamicSerializer;
import datawave.query.function.KryoCVAwareSerializableSerializer;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;

import java.math.BigDecimal;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;

class KryoObjectPoolFactory extends BasePooledObjectFactory<KryoEntry> {
    @Override
    public KryoEntry create() throws Exception {
        Kryo kryo = new Kryo();
        // Log.set(Log.LEVEL_TRACE);
        
        kryo.setReferences(false);

//        kryo.register(IpV4Address.class, new IPV4AddressSerializer());
//        kryo.register(IpV6Address.class, new IPV6AddressSerializer());

        Map<Class<?>,DelegateTypeSerializer<?,? extends BaseType<?>>> map = new LinkedHashMap<>();
        map.put(DateType.class, new DelegateTypeSerializer<>(new DefaultSerializers.DateSerializer(), Date.class));
        map.put(GeoLatType.class, new DelegateTypeSerializer<>(new DefaultSerializers.StringSerializer(), String.class));
        map.put(GeoLonType.class, new DelegateTypeSerializer<>(new DefaultSerializers.StringSerializer(), String.class));
        map.put(GeometryType.class, new DelegateTypeSerializer<>(new GeometrySerializer(Geometry.class, org.locationtech.jts.geom.Geometry.class),
                        Geometry.class));
        map.put(GeoType.class, new DelegateTypeSerializer<>(new DefaultSerializers.StringSerializer(), String.class));
        map.put(HexStringType.class, new DelegateTypeSerializer<>(new DefaultSerializers.StringSerializer(), String.class));
        map.put(HitTermType.class, new DelegateTypeSerializer<>(new DefaultSerializers.StringSerializer(), String.class));
        map.put(IpAddress.class, new DelegateTypeSerializer<>(new IpAddressDynamicSerializer(), IpAddress.class));
        map.put(LcType.class, new DelegateTypeSerializer<>(new DefaultSerializers.StringSerializer(), String.class));
        map.put(LcNoDiacriticsType.class, new DelegateTypeSerializer<>(new DefaultSerializers.StringSerializer(), String.class));
        map.put(MacAddressType.class, new DelegateTypeSerializer<>(new DefaultSerializers.StringSerializer(), String.class));
        map.put(NoOpType.class, new DelegateTypeSerializer<>(new DefaultSerializers.StringSerializer(), String.class));
        map.put(NumberType.class, new DelegateTypeSerializer<>(new DefaultSerializers.BigDecimalSerializer(), BigDecimal.class));
        map.put(PointType.class, new DelegateTypeSerializer<>(new GeometrySerializer(Point.class, org.locationtech.jts.geom.Point.class), Point.class));
        map.put(RawDateType.class, new DelegateTypeSerializer<>(new DefaultSerializers.StringSerializer(), String.class));
        map.put(StringType.class, new DelegateTypeSerializer<>(new DefaultSerializers.StringSerializer(), String.class));
        map.put(RawDateType.class, new DelegateTypeSerializer<>(new DefaultSerializers.StringSerializer(), String.class));
        map.put(TrimLeadingZerosType.class, new DelegateTypeSerializer<>(new DefaultSerializers.StringSerializer(), String.class));
        
        // register the class and default serializers for the type attributes
        // map.forEach((k, v) -> kryo.register(k, v));
        
        // set the serializer for attribute class
        kryo.addDefaultSerializer(Attribute.class, new KryoCVAwareSerializableSerializer(true));
        
        KryoDocumentOptions documentOptions = new KryoDocumentOptions();
        // map.forEach((k, v) -> documentOptions.addSerializer(k, v));
        kryo.getContext().put(KryoDocumentOptions.CACHE_KEY, documentOptions);
        kryo.setAutoReset(false);
        KryoEntry kryoEntry = new KryoEntry(kryo);
        return kryoEntry;
    }
    
    @Override
    public PooledObject<KryoEntry> wrap(KryoEntry kryoEntry) {
        return new DefaultPooledObject<>(kryoEntry);
    }
}

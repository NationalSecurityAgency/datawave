package datawave.query.attributes.serialization.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import datawave.data.type.util.AbstractGeometry;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.*;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

public class GeometrySerializer<G extends Geometry,T extends AbstractGeometry<G>> extends Serializer<T> {
    private final static boolean INCLUDE_SRID = true;
    
    private final Constructor<T> geometryConstructor;
    
    public GeometrySerializer(Class<T> typeClass, Class<G> geometryClass) {
        try {
            this.geometryConstructor = typeClass.getConstructor(geometryClass);
        } catch (NoSuchMethodException e) {
            throw new IllegalStateException(e);
        }
    }
    
    @Override
    public void write(Kryo kryo, Output output, T geom) {
        org.locationtech.jts.geom.Geometry jtsGeometry = geom.getJTSGeometry();
        
        // WKBWriter writer = new WKBWriter(jtsGeometry.getDimension(), INCLUDE_SRID);
        WKBWriter writer = new WKBWriter();
        OutputStreamOutStream writerOut = new OutputStreamOutStream(output);
        try {
            writer.write(jtsGeometry, writerOut);
        } catch (IOException e) {
            throw new KryoException(e);
        }
    }
    
    @Override
    public T read(Kryo kryo, Input input, Class<T> geometryClass) {
        WKBReader reader = new WKBReader();
        InStream readerIn = new InputStreamInStream(input);
        org.locationtech.jts.geom.Geometry jtsGeometry;
        try {
            jtsGeometry = reader.read(readerIn);
        } catch (IOException | ParseException e) {
            throw new KryoException(e);
        }
        try {
            return (T) geometryConstructor.newInstance(jtsGeometry);
        } catch (InvocationTargetException | InstantiationException | IllegalAccessException e) {
            throw new KryoException(e);
        }
    }
    
    private static class GeometrySerializerCache {
        private WKBReader reader;
        private WKBWriter writer;
    }
}

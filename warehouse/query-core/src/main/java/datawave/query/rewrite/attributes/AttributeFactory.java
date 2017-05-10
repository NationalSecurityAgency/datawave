package datawave.query.rewrite.attributes;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

import datawave.data.type.LcNoDiacriticsType;
import datawave.data.type.NoOpType;
import datawave.data.type.Type;
import datawave.query.util.TypeMetadata;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Sets;

public class AttributeFactory {
    private static final Logger log = Logger.getLogger(AttributeFactory.class);
    
    protected static LoadingCache<String,Class<?>> clazzCache = CacheBuilder.newBuilder().maximumSize(128).expireAfterAccess(1, TimeUnit.HOURS)
                    .build(new CacheLoader<String,Class<?>>() {
                        @Override
                        public Class<?> load(String clazz) throws Exception {
                            return Class.forName(clazz);
                        }
                    });
    
    private final TypeMetadata typeMetadata;
    
    private String defaultType = NoOpType.class.getName();
    private Class<?> mostGeneralType = LcNoDiacriticsType.class;
    
    public AttributeFactory(TypeMetadata typeMetadata) {
        this.typeMetadata = typeMetadata;
    }
    
    public AttributeFactory(TypeMetadata typeMetadata, String defaultType) {
        this(typeMetadata);
        this.defaultType = defaultType;
    }
    
    private String extractIngestDataTypeFromKey(Key key) {
        Text cf = new Text();
        key.getColumnFamily(cf);
        ByteBuffer b = ByteBuffer.wrap(cf.getBytes(), 0, cf.getLength());
        int endPos = 0;
        while (b.hasRemaining()) {
            if (b.get() == 0)
                break;
            endPos++;
        }
        cf.set(cf.getBytes(), 0, endPos);
        return cf.toString();
    }
    
    public Attribute<?> create(String fieldName, String data, Key key, boolean toKeep) {
        
        return this.create(fieldName, data, key, extractIngestDataTypeFromKey(key), toKeep);
    }
    
    public Attribute<?> create(String fieldName, String data, Key key, String ingestType, boolean toKeep) {
        
        Collection<String> dataTypes = this.typeMetadata.getTypeMetadata(fieldName, ingestType);
        if (dataTypes.isEmpty()) {
            return new NoOpContent(data, key, toKeep);
        }
        
        try {
            if (null == dataTypes || dataTypes.isEmpty()) {
                Class<?> dataTypeClass = clazzCache.get(this.defaultType);
                return getAttribute(dataTypeClass, fieldName, data, key, toKeep);
            } else if (1 == dataTypes.size()) {
                String dataType = dataTypes.iterator().next();
                Class<?> dataTypeClass = clazzCache.get(dataType);
                return getAttribute(dataTypeClass, fieldName, data, key, toKeep);
            } else {
                HashSet<Attribute<? extends Comparable<?>>> attrSet = Sets.newHashSet();
                
                for (String dataType : dataTypes) {
                    Class<?> dataTypeClass = clazzCache.get(dataType);
                    Attribute<?> attribute = getAttribute(dataTypeClass, fieldName, data, key, toKeep);
                    // if there is more than one dataType, mark the mostGeneral one as toKeep=false, leaving the more specific type(s)
                    if (dataTypeClass.isAssignableFrom(this.mostGeneralType)) {
                        attribute.setToKeep(false);
                    }
                    attrSet.add(attribute);
                }
                
                return new Attributes(attrSet, toKeep);
            }
        } catch (Exception ex) {
            log.error("Could not create Attribute for " + fieldName + " and " + data, ex);
            throw new RuntimeException("Could not create Attribute for " + fieldName + " and " + data, ex);
        }
    }
    
    protected Attribute<?> getAttribute(Class<?> dataTypeClass, String fieldName, String data, Key key, boolean toKeep) throws Exception {
        Type<?> type = (Type<?>) dataTypeClass.newInstance();
        try {
            type.setDelegateFromString(data);
            return new TypeAttribute(type, key, toKeep);
        } catch (Exception ex) {
            
            if (ex instanceof IllegalArgumentException) {
                log.warn("Could not parse " + fieldName + " = '" + data + "', resorting to a NoOpType");
                return new TypeAttribute(new NoOpType(data), key, toKeep);
            } else {
                log.error("Could not create Attribute for " + fieldName + " and " + data, ex);
                throw new IllegalArgumentException("Could not create Attribute for " + fieldName + " and " + data, ex);
            }
            
        }
    }
}

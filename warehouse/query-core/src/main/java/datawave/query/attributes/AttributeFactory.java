package datawave.query.attributes;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import datawave.data.type.LcNoDiacriticsType;
import datawave.data.type.NoOpType;
import datawave.data.type.Type;
import datawave.query.util.TypeMetadata;

import org.apache.accumulo.core.data.Key;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Sets;

import javax.annotation.Nullable;

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
    private static final List<Class<?>> mostGeneralTypes = Collections
                    .unmodifiableList(Lists.<Class<?>> newArrayList(NoOpType.class, LcNoDiacriticsType.class));
    
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
        
        return this.create(fieldName, data, key, extractIngestDataTypeFromKey(key), toKeep, false);
    }
    
    public Attribute<?> create(String fieldName, String data, Key key, boolean toKeep, boolean isComposite) {
        
        return this.create(fieldName, data, key, extractIngestDataTypeFromKey(key), toKeep, isComposite);
    }
    
    public Attribute<?> create(String fieldName, String data, Key key, String ingestType, boolean toKeep) {
        return this.create(fieldName, data, key, ingestType, toKeep, false);
    }
    
    public Attribute<?> create(String fieldName, String data, Key key, String ingestType, boolean toKeep, boolean isComposite) {
        
        Collection<String> dataTypes = (isComposite) ? Arrays.asList(NoOpType.class.getName()) : this.typeMetadata.getTypeMetadata(fieldName, ingestType);
        
        try {
            if (null == dataTypes || dataTypes.isEmpty()) {
                Class<?> dataTypeClass = clazzCache.get(this.defaultType);
                return getAttribute(dataTypeClass, fieldName, data, key, toKeep);
            } else if (1 == dataTypes.size()) {
                String dataType = dataTypes.iterator().next();
                Class<?> dataTypeClass = clazzCache.get(dataType);
                return getAttribute(dataTypeClass, fieldName, data, key, toKeep);
            } else {
                
                Iterable<Class<?>> typeClasses = Iterables.transform(dataTypes, new Function<String,Class<?>>() {
                    @Nullable
                    @Override
                    public Class<?> apply(@Nullable String s) {
                        try {
                            return clazzCache.get(s);
                        } catch (ExecutionException e) {
                            throw new RuntimeException("could not make a class from " + s, e);
                        }
                    }
                });
                
                Collection<Class<?>> keepers = AttributeFactory.getKeepers(typeClasses);
                
                HashSet<Attribute<? extends Comparable<?>>> attrSet = Sets.newHashSet();
                
                for (String dataType : dataTypes) {
                    Class<?> dataTypeClass = clazzCache.get(dataType);
                    Attribute<?> attribute = getAttribute(dataTypeClass, fieldName, data, key, toKeep);
                    // if there is more than one dataType, mark the mostGeneral one as toKeep=false, leaving the more specific type(s)
                    // if the class is not a member of 'keepers', then set toKeep to false
                    if (!keepers.contains(dataTypeClass)) {
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
    
    public static Collection<Class<?>> getKeepers(Iterable<Class<?>> finders) {
        Collection<Class<?>> keepers = Sets.newHashSet(finders);
        List<Class<?>> losers = AttributeFactory.mostGeneralTypes;
        if (keepers.size() == 1) {
            // do not remove anything
            return keepers;
        }
        
        // created and populated only for trace debug
        Collection<Class<?>> weepers = null;
        if (log.isTraceEnabled()) {
            weepers = Sets.newHashSet();
        }
        for (Class<?> loser : losers) {
            // try one at a time until only one remains
            // the list of losers is in priority order, lowest at the beginning
            boolean removed = keepers.remove(loser);
            if (log.isTraceEnabled() && removed) {
                weepers.add(loser);
            }
            if (keepers.size() == 1) {
                // have to stop so i don't risk removing _everything_ from keepers
                break;
            }
        }
        if (log.isTraceEnabled()) {
            log.trace("finders: " + finders);
            log.trace("keepers: " + keepers);
            log.trace("losers: " + losers);
            log.trace("weepers: " + weepers);
        }
        return keepers;
    }
    
}

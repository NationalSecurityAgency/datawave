package datawave.query.composite;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import datawave.util.DateSchema;
import datawave.util.StringMultimapSchema;
import io.protostuff.Input;
import io.protostuff.LinkedBuffer;
import io.protostuff.Message;
import io.protostuff.Output;
import io.protostuff.ProtobufIOUtil;
import io.protostuff.Schema;
import io.protostuff.StringMapSchema;

import java.io.IOException;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Composite metadata is used when determining how to treat composite terms and ranges when they are encountered in the QueryIterator. This metadata represents
 * the mapping of composite fields to their component fields, separated by ingest type. This class is also serializable using the protostuff api.
 *
 */
public class CompositeMetadata implements Message<CompositeMetadata> {
    
    private static final LinkedBuffer linkedBuffer;
    
    static {
        linkedBuffer = LinkedBuffer.allocate(4096);
    }
    
    protected Map<String,Multimap<String,String>> compositeFieldMapByType;
    protected Map<String,Map<String,Date>> compositeTransitionDatesByType;
    protected Map<String,Map<String,String>> compositeFieldSeparatorsByType;
    
    public CompositeMetadata() {
        this.compositeFieldMapByType = new HashMap<>();
        this.compositeTransitionDatesByType = new HashMap<>();
        this.compositeFieldSeparatorsByType = new HashMap<>();
    }
    
    public Map<String,Multimap<String,String>> getCompositeFieldMapByType() {
        return compositeFieldMapByType;
    }
    
    public void setCompositeFieldMapByType(Map<String,Multimap<String,String>> compositeFieldMapByType) {
        this.compositeFieldMapByType = compositeFieldMapByType;
    }
    
    public void setCompositeFieldMappingByType(String ingestType, String compositeField, Collection<String> componentFields) {
        Multimap<String,String> compositeFieldMap;
        if (!compositeFieldMapByType.containsKey(ingestType)) {
            compositeFieldMap = ArrayListMultimap.create();
            compositeFieldMapByType.put(ingestType, compositeFieldMap);
        } else {
            compositeFieldMap = compositeFieldMapByType.get(ingestType);
        }
        
        if (!compositeFieldMap.containsKey(compositeField))
            compositeFieldMap.putAll(compositeField, componentFields);
        else
            compositeFieldMap.replaceValues(compositeField, componentFields);
    }
    
    public Map<String,Map<String,Date>> getCompositeTransitionDatesByType() {
        return compositeTransitionDatesByType;
    }
    
    public void setCompositeTransitionDatesByType(Map<String,Map<String,Date>> compositeTransitionDatesByType) {
        this.compositeTransitionDatesByType = compositeTransitionDatesByType;
    }
    
    public void addCompositeTransitionDateByType(String ingestType, String compositeFieldName, Date transitionDate) {
        Map<String,Date> compositeTransitionDateMap;
        if (!compositeTransitionDatesByType.containsKey(ingestType)) {
            compositeTransitionDateMap = new HashMap<>();
            compositeTransitionDatesByType.put(ingestType, compositeTransitionDateMap);
        } else {
            compositeTransitionDateMap = compositeTransitionDatesByType.get(ingestType);
        }
        
        compositeTransitionDateMap.put(compositeFieldName, transitionDate);
    }
    
    public Map<String,Map<String,String>> getCompositeFieldSeparatorsByType() {
        return compositeFieldSeparatorsByType;
    }
    
    public void setCompositeFieldSeparatorsByType(Map<String,Map<String,String>> compositeFieldSeparatorsByType) {
        this.compositeFieldSeparatorsByType = compositeFieldSeparatorsByType;
    }
    
    private void setCompositeFieldSeparatorsByTypeInternal(Map<String,Multimap<String,String>> compositeFieldSeparatorsByType) {
        this.compositeFieldSeparatorsByType = new HashMap<>();
        compositeFieldSeparatorsByType.forEach((ingestType, v) -> v.entries().forEach(
                        entry -> addCompositeFieldSeparatorByType(ingestType, entry.getKey(), entry.getValue())));
    }
    
    public void addCompositeFieldSeparatorByType(String ingestType, String compositeFieldName, String separator) {
        Map<String,String> compositeFieldSeparatorMap;
        if (!compositeFieldSeparatorsByType.containsKey(ingestType)) {
            compositeFieldSeparatorMap = new HashMap<>();
            compositeFieldSeparatorsByType.put(ingestType, compositeFieldSeparatorMap);
        } else {
            compositeFieldSeparatorMap = compositeFieldSeparatorsByType.get(ingestType);
        }
        
        compositeFieldSeparatorMap.put(compositeFieldName, separator);
    }
    
    public boolean isEmpty() {
        return (compositeFieldMapByType == null || compositeFieldMapByType.isEmpty())
                        && (compositeTransitionDatesByType == null || compositeTransitionDatesByType.isEmpty())
                        && (compositeFieldSeparatorsByType == null || compositeFieldSeparatorsByType.isEmpty());
    }
    
    public CompositeMetadata filter(Set<String> componentFields) {
        Set<String> ingestTypes = new HashSet<>();
        ingestTypes.addAll(this.compositeFieldMapByType.keySet());
        ingestTypes.addAll(this.compositeTransitionDatesByType.keySet());
        ingestTypes.addAll(this.compositeFieldSeparatorsByType.keySet());
        return filter(ingestTypes, componentFields);
    }
    
    public CompositeMetadata filter(Set<String> ingestTypes, Set<String> componentFields) {
        if (!isEmpty()) {
            CompositeMetadata compositeMetadata = new CompositeMetadata();
            for (String ingestType : ingestTypes) {
                for (String componentField : componentFields) {
                    if (this.compositeFieldMapByType.containsKey(ingestType)) {
                        for (String compositeField : this.compositeFieldMapByType.get(ingestType).keySet()) {
                            if (this.compositeFieldMapByType.get(ingestType).get(compositeField).contains(componentField)) {
                                compositeMetadata.setCompositeFieldMappingByType(ingestType, compositeField,
                                                this.compositeFieldMapByType.get(ingestType).get(compositeField));
                                
                                if (this.compositeTransitionDatesByType.containsKey(ingestType)
                                                && this.compositeTransitionDatesByType.get(ingestType).containsKey(compositeField))
                                    compositeMetadata.addCompositeTransitionDateByType(ingestType, compositeField,
                                                    this.compositeTransitionDatesByType.get(ingestType).get(compositeField));
                                
                                if (this.compositeFieldSeparatorsByType.containsKey(ingestType)
                                                && this.compositeFieldSeparatorsByType.get(ingestType).containsKey(compositeField))
                                    compositeMetadata.addCompositeFieldSeparatorByType(ingestType, compositeField,
                                                    this.compositeFieldSeparatorsByType.get(ingestType).get(compositeField));
                            }
                        }
                    }
                }
            }
            return compositeMetadata;
        }
        return this;
    }
    
    public static byte[] toBytes(CompositeMetadata compositeMetadata) {
        if (compositeMetadata != null && !compositeMetadata.isEmpty()) {
            byte[] bytes = ProtobufIOUtil.toByteArray(compositeMetadata, COMPOSITE_METADATA_SCHEMA, linkedBuffer);
            linkedBuffer.clear();
            return bytes;
        } else
            return new byte[] {};
    }
    
    public static CompositeMetadata fromBytes(byte[] compositeMetadataBytes) {
        CompositeMetadata compositeMetadata = COMPOSITE_METADATA_SCHEMA.newMessage();
        ProtobufIOUtil.mergeFrom(compositeMetadataBytes, compositeMetadata, COMPOSITE_METADATA_SCHEMA);
        return compositeMetadata;
    }
    
    @Override
    public Schema<CompositeMetadata> cachedSchema() {
        return COMPOSITE_METADATA_SCHEMA;
    }
    
    public static Schema<CompositeMetadata> COMPOSITE_METADATA_SCHEMA = new Schema<CompositeMetadata>() {
        
        public static final String COMPOSITE_FIELD_MAPPING_BY_TYPE = "compositeFieldMappingByType";
        public static final String COMPOSITE_TRANSITION_DATE_BY_TYPE = "compositeTransitionDatesByType";
        public static final String COMPOSITE_FIELD_SEPARATOR_BY_TYPE = "compositeFieldSeparatorsByType";
        
        public Schema<Map<String,Multimap<String,String>>> compositeFieldMappingByTypeSchema = new StringMapSchema<>(new StringMultimapSchema());
        public Schema<Map<String,Map<String,Date>>> compositeTransitionDateByTypeSchema = new StringMapSchema<>(new StringMapSchema<>(new DateSchema()));
        public Schema<Map<String,Multimap<String,String>>> compositeFieldSeparatorsByTypeSchema = new StringMapSchema<>(new StringMultimapSchema());
        
        @Override
        public String getFieldName(int number) {
            switch (number) {
                case 1:
                    return COMPOSITE_FIELD_MAPPING_BY_TYPE;
                case 2:
                    return COMPOSITE_TRANSITION_DATE_BY_TYPE;
                case 3:
                    return COMPOSITE_FIELD_SEPARATOR_BY_TYPE;
                default:
                    return null;
            }
        }
        
        @Override
        public int getFieldNumber(String name) {
            switch (name) {
                case COMPOSITE_FIELD_MAPPING_BY_TYPE:
                    return 1;
                case COMPOSITE_TRANSITION_DATE_BY_TYPE:
                    return 2;
                case COMPOSITE_FIELD_SEPARATOR_BY_TYPE:
                    return 3;
                default:
                    return 0;
            }
        }
        
        @Override
        public boolean isInitialized(CompositeMetadata compositeMetadata) {
            return true;
        }
        
        @Override
        public CompositeMetadata newMessage() {
            return new CompositeMetadata();
        }
        
        @Override
        public String messageName() {
            return CompositeMetadata.class.getSimpleName();
        }
        
        @Override
        public String messageFullName() {
            return CompositeMetadata.class.getName();
        }
        
        @Override
        public Class<? super CompositeMetadata> typeClass() {
            return CompositeMetadata.class;
        }
        
        @Override
        public void mergeFrom(Input input, CompositeMetadata compositeMetadata) throws IOException {
            for (int number = input.readFieldNumber(this);; number = input.readFieldNumber(this)) {
                switch (number) {
                    case 0:
                        return;
                    case 1:
                        compositeMetadata.setCompositeFieldMapByType(input.mergeObject(null, compositeFieldMappingByTypeSchema));
                        break;
                    case 2:
                        compositeMetadata.setCompositeTransitionDatesByType(input.mergeObject(null, compositeTransitionDateByTypeSchema));
                        break;
                    case 3:
                        compositeMetadata.setCompositeFieldSeparatorsByTypeInternal(input.mergeObject(null, compositeFieldSeparatorsByTypeSchema));
                        break;
                    default:
                        input.handleUnknownField(number, this);
                }
            }
        }
        
        @Override
        public void writeTo(Output output, CompositeMetadata compositeMetadata) throws IOException {
            if (compositeMetadata.getCompositeFieldMapByType() != null)
                output.writeObject(1, compositeMetadata.getCompositeFieldMapByType(), compositeFieldMappingByTypeSchema, false);
            if (compositeMetadata.getCompositeTransitionDatesByType() != null)
                output.writeObject(2, compositeMetadata.getCompositeTransitionDatesByType(), compositeTransitionDateByTypeSchema, false);
            if (compositeMetadata.getCompositeFieldSeparatorsByType() != null) {
                Map<String,Multimap<String,String>> newMap = new HashMap<>();
                compositeMetadata.getCompositeFieldSeparatorsByType().forEach((ingestType, map) -> newMap.put(ingestType, mapToMultimap(map)));
                output.writeObject(3, newMap, compositeFieldSeparatorsByTypeSchema, false);
            }
        }
        
        private Multimap<String,String> mapToMultimap(Map<String,String> map) {
            Multimap<String,String> multimap = LinkedListMultimap.create();
            map.forEach(multimap::put);
            return multimap;
        }
    };
}

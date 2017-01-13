package nsa.datawave.query.util;

import com.google.common.base.Splitter;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class TypeMetadata implements Serializable {
    
    private Set<String> ingestTypes = Sets.newHashSet();
    
    private Set<String> fieldNames = Sets.newHashSet();
    
    protected Map<String,Multimap<String,String>> typeMetadata;
    
    public static final Multimap<String,String> emptyMap = HashMultimap.create();
    
    public TypeMetadata() {
        typeMetadata = Maps.newHashMap();
    }
    
    public TypeMetadata(String in) {
        typeMetadata = Maps.newHashMap();
        this.fromString(in);
    }
    
    public TypeMetadata(TypeMetadata in) {
        typeMetadata = Maps.newHashMap();
        // make sure we do a deep copy to avoid access issues later
        for (Map.Entry<String,Multimap<String,String>> entry : in.typeMetadata.entrySet()) {
            this.typeMetadata.put(entry.getKey(), HashMultimap.create(entry.getValue()));
        }
        this.ingestTypes.addAll(in.ingestTypes);
    }
    
    public void addForAllIngestTypes(Map<String,Set<String>> map) {
        for (String fieldName : map.keySet()) {
            for (String ingestType : ingestTypes) {
                this.put(fieldName, ingestType, map.get(fieldName));
            }
        }
    }
    
    private TypeMetadata put(String fieldName, String ingestType, Collection<String> types) {
        addTypeMetadata(fieldName, ingestType, types);
        return this;
    }
    
    public TypeMetadata put(String fieldName, String ingestType, String type) {
        if (null == this.typeMetadata.get(ingestType)) {
            Multimap<String,String> map = HashMultimap.create();
            this.typeMetadata.put(ingestType, map);
        }
        addTypeMetadata(fieldName, ingestType, type);
        return this;
    }
    
    private void addTypeMetadata(String fieldName, String ingestType, Collection<String> types) {
        this.ingestTypes.add(ingestType);
        fieldNames.add(fieldName);
        if (null == this.typeMetadata.get(ingestType)) {
            Multimap<String,String> typeMap = HashMultimap.create();
            typeMap.putAll(fieldName, types);
            this.typeMetadata.put(ingestType, typeMap);
        } else {
            this.typeMetadata.get(ingestType).putAll(fieldName, types);
        }
    }
    
    private void addTypeMetadata(String fieldName, String ingestType, String type) {
        this.ingestTypes.add(ingestType);
        fieldNames.add(fieldName);
        if (null == this.typeMetadata.get(ingestType)) {
            Multimap<String,String> typeMap = HashMultimap.create();
            typeMap.put(fieldName, type);
            this.typeMetadata.put(ingestType, typeMap);
        } else {
            this.typeMetadata.get(ingestType).put(fieldName, type);
        }
    }
    
    public Collection<String> getTypeMetadata(String fieldName, String ingestType) {
        Multimap<String,String> map = this.typeMetadata.get(ingestType);
        if (null == map) {
            return Collections.EMPTY_SET;
        }
        // defensive copy
        return Sets.newHashSet(map.get(fieldName));
    }
    
    /**
     * returns a multimap of field name to datatype name ingest type names are not included
     *
     * @return
     */
    public Multimap<String,String> fold() {
        Multimap<String,String> map = HashMultimap.create();
        for (Multimap<String,String> entry : this.typeMetadata.values()) {
            // defensive copy
            map.putAll(HashMultimap.create(entry));
        }
        return map;
    }
    
    /**
     * returns a multimap of field name to datatype name, filtered on provided ingest type names ingest type names are not included
     *
     * @param ingestTypeFilter
     * @return
     */
    public Multimap<String,String> fold(Set<String> ingestTypeFilter) {
        if (ingestTypeFilter == null || ingestTypeFilter.isEmpty()) {
            return this.fold();
        }
        Multimap<String,String> map = HashMultimap.create();
        
        for (String type : ingestTypeFilter) {
            // defensive copy
            map.putAll(HashMultimap.create(this.typeMetadata.get(type)));
        }
        return map;
    }
    
    public int size() {
        return this.typeMetadata.size();
    }
    
    public Set<String> keySet() {
        return fieldNames;
    }
    
    public TypeMetadata filter(Set<String> datatypeFilter) {
        if (datatypeFilter == null || datatypeFilter.isEmpty())
            return new TypeMetadata(this);
        Map<String,Multimap<String,String>> localMap = Maps.newHashMap();
        
        for (String type : datatypeFilter) {
            
            Multimap<String,String> map = HashMultimap.create();
            if (null != (this.typeMetadata.get(type))) {
                // defensive copy
                map.putAll(HashMultimap.create(this.typeMetadata.get(type)));
            }
            localMap.put(type, map);
        }
        
        TypeMetadata typeMetadata = new TypeMetadata();
        typeMetadata.ingestTypes = datatypeFilter;
        typeMetadata.typeMetadata.putAll(localMap);
        return typeMetadata;
    }
    
    public boolean isEmpty() {
        return this.keySet().isEmpty();
    }
    
    public String toString() {
        StringBuilder sb = new StringBuilder();
        
        Set<String> fieldNames = Sets.newHashSet();
        for (String ingestType : typeMetadata.keySet()) {
            fieldNames.addAll(typeMetadata.get(ingestType).keySet());
        }
        
        for (String fieldName : fieldNames) {
            if (sb.length() > 0) {
                sb.append(';');
            }
            
            sb.append(fieldName).append(':');
            sb.append('[');
            boolean firstField = true;
            for (String ingestType : typeMetadata.keySet()) {
                if (!typeMetadata.get(ingestType).containsKey(fieldName))
                    continue;
                if (!firstField)
                    sb.append(';');
                firstField = false;
                sb.append(ingestType);
                sb.append(':');
                boolean first = true;
                for (String type : typeMetadata.get(ingestType).get(fieldName)) {
                    if (!first)
                        sb.append(',');
                    sb.append(type);
                    first = false;
                }
            }
            sb.append(']');
        }
        
        return sb.toString();
    }
    
    private void fromString(String data) {
        // was:
        // field1:a,b;field2:d,e;field3:y,z
        
        // post-fix: String should look like this:
        // field1:[type1:a,b;type2:b];field2:[type1:a,b;type2:a,c]
        fieldNames = Sets.newHashSet();
        String[] entries = parse(data, ';');
        for (String entry : entries) {
            String[] entrySplits = parse(entry, ':');
            if (2 != entrySplits.length) {
                // Do nothing
            } else {
                // entrySplits[1] looks like this:
                // [type1:a,b;type2:b] - split it on the ';'
                // get rid of the leading and trailing brackets:
                entrySplits[1] = entrySplits[1].substring(1, entrySplits[1].length() - 1);
                String[] values = parse(entrySplits[1], ';');
                
                for (String value : values) {
                    
                    String[] vs = Iterables.toArray(Splitter.on(':').omitEmptyStrings().trimResults().split(value), String.class);
                    
                    Multimap<String,String> mm = typeMetadata.get(vs[0]);
                    if (null == mm) {
                        mm = HashMultimap.create();
                        typeMetadata.put(vs[0], mm);
                    }
                    
                    String[] rhs = Iterables.toArray(Splitter.on(',').omitEmptyStrings().trimResults().split(vs[1]), String.class);
                    this.ingestTypes.add(vs[0]);
                    for (String r : rhs) {
                        mm.put(entrySplits[0], r);
                    }
                }
                fieldNames.add(entrySplits[0]);
            }
        }
    }
    
    private static String[] parse(String in, char c) {
        List<String> list = Lists.newArrayList();
        boolean inside = false;
        int start = 0;
        for (int i = 0; i < in.length(); i++) {
            if (in.charAt(i) == '[')
                inside = true;
            if (in.charAt(i) == ']')
                inside = false;
            if (in.charAt(i) == c && !inside) {
                list.add(in.substring(start, i));
                start = i + 1;
            }
        }
        list.add(in.substring(start));
        return Iterables.toArray(list, String.class);
    }
    
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((typeMetadata == null) ? 0 : typeMetadata.hashCode());
        return result;
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        TypeMetadata other = (TypeMetadata) obj;
        if (typeMetadata == null) {
            if (other.typeMetadata != null)
                return false;
        } else if (!toString().equals(obj.toString())) {
            return false;
        }
        return true;
    }
    
    private void writeObject(ObjectOutputStream out) throws Exception {
        out.writeObject(this.toString());
    }
    
    private void readObject(ObjectInputStream in) throws Exception {
        this.ingestTypes = Sets.newHashSet();
        this.typeMetadata = Maps.newHashMap();
        this.fromString((String) in.readObject());
    }
    
    public static final TypeMetadata EMPTY_TYPE_METADATA = new EmptyTypeMetadata();
    
    public static TypeMetadata emptyTypeMetadata() {
        return EMPTY_TYPE_METADATA;
    }
    
    private static class EmptyTypeMetadata extends TypeMetadata implements Serializable {
        
        final Multimap<String,String> EMPTY_MULTIMAP = new ImmutableMultimap.Builder().build();
        
        public Collection<String> getTypeMetadata(String fieldName, String ingestType) {
            return Collections.emptySet();
        }
        
        /**
         * returns a multimap of field name to datatype name ingest type names are not included
         * 
         * @return
         */
        public Multimap<String,String> fold() {
            return EMPTY_MULTIMAP;
        }
        
        /**
         * returns a multimap of field name to datatype name, filtered on provided ingest type names ingest type names are not included
         * 
         * @param ingestTypeFilter
         * @return
         */
        public Multimap<String,String> fold(Set<String> ingestTypeFilter) {
            return EMPTY_MULTIMAP;
        }
        
        public Set<Entry<String,Multimap<String,String>>> entrySet() {
            return Collections.emptySet();
        }
        
        public Set<String> keySet() {
            return Collections.emptySet();
        }
        
        public TypeMetadata filter(Set<String> datatypeFilter) {
            return this;
        }
        
        public boolean equals(Object o) {
            return (o instanceof TypeMetadata) && ((TypeMetadata) o).isEmpty();
        }
        
        public int hashCode() {
            return 0;
        }
        
        // Preserves singleton property
        private Object readResolve() {
            return EMPTY_TYPE_METADATA;
        }
    }
}

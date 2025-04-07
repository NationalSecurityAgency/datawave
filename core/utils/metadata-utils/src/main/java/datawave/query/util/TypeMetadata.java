package datawave.query.util;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

import com.google.common.base.Splitter;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

public class TypeMetadata implements Serializable {
    
    private Set<String> ingestTypes = new TreeSet<>();
    
    private Set<String> fieldNames = new TreeSet<>();
    
    public Map<String,Integer> getIngestTypesMiniMap() {
        return ingestTypesMiniMap;
    }
    
    public void setIngestTypesMiniMap(Map<String,Integer> ingestTypesMiniMap) {
        this.ingestTypesMiniMap = ingestTypesMiniMap;
    }
    
    public Map<String,Integer> getDataTypesMiniMap() {
        return dataTypesMiniMap;
    }
    
    public void setDataTypesMiniMap(Map<String,Integer> dataTypesMiniMap) {
        this.dataTypesMiniMap = dataTypesMiniMap;
    }
    
    private Map<String,Integer> ingestTypesMiniMap;
    private Map<String,Integer> dataTypesMiniMap;
    
    // <ingestType, <fieldName, DataType(s)>>
    protected Map<String,Multimap<String,String>> typeMetadata;
    
    public static final Multimap<String,String> emptyMap = HashMultimap.create();
    
    private static final String INGESTTYPE_PREFIX = "dts";
    private static final String DATATYPES_PREFIX = "types";
    
    public TypeMetadata() {
        typeMetadata = Maps.newHashMap();
        ingestTypesMiniMap = new TreeMap<>();
        dataTypesMiniMap = new TreeMap<>();
    }
    
    public TypeMetadata(String in) {
        typeMetadata = Maps.newHashMap();
        ingestTypesMiniMap = new TreeMap<>();
        dataTypesMiniMap = new TreeMap<>();
        this.fromString(in);
    }
    
    public TypeMetadata(TypeMetadata in) {
        typeMetadata = Maps.newHashMap();
        ingestTypesMiniMap = new TreeMap<>();
        dataTypesMiniMap = new TreeMap<>();
        // make sure we do a deep copy to avoid access issues later
        for (Entry<String,Multimap<String,String>> entry : in.typeMetadata.entrySet()) {
            this.typeMetadata.put(entry.getKey(), HashMultimap.create(entry.getValue()));
        }
        this.ingestTypes.addAll(in.ingestTypes);
        this.fieldNames.addAll(in.fieldNames);
        this.ingestTypesMiniMap.putAll(in.getIngestTypesMiniMap());
        this.dataTypesMiniMap.putAll(in.getDataTypesMiniMap());
    }
    
    /**
     * Creates a copy of this type metadata object, reducing it down to the set of provided fields
     *
     * @param fields
     *            a set of fields which act as a filter
     * @return a copy of the TypeMetadata filtered down to a set of fields
     */
    public TypeMetadata reduce(Set<String> fields) {
        TypeMetadata reduced = new TypeMetadata();
        for (Entry<String,Multimap<String,String>> entry : typeMetadata.entrySet()) {
            final String ingestType = entry.getKey();
            for (Entry<String,String> element : entry.getValue().entries()) {
                final String field = element.getKey();
                final String normalizer = element.getValue();
                if (fields.contains(field)) {
                    reduced.addTypeMetadata(field, ingestType, normalizer);
                }
            }
        }
        return reduced;
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
        this.fieldNames.add(fieldName);
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
        this.fieldNames.add(fieldName);
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
            return Collections.emptySet();
        }
        // defensive copy
        return Sets.newHashSet(map.get(fieldName));
    }
    
    /**
     * Returns a set of all Normalizer names associated with the given fieldName. This is similar to calling .fold().get(fieldName)
     *
     * @param fieldName
     *            a field name against which to search for any associated Normalizer Types
     * @return a set of strings of associated Normalizer Types
     */
    public Set<String> getNormalizerNamesForField(String fieldName) {
        if (fieldName == null || fieldName.isEmpty()) {
            return Collections.emptySet();
        }
        
        Set<String> normalizers = new HashSet<>();
        for (Multimap<String,String> entry : this.typeMetadata.values()) {
            normalizers.addAll(entry.get(fieldName));
        }
        
        return normalizers;
    }
    
    /**
     * Returns a set of all dataType names associated with the given fieldName
     *
     * @param fieldName
     *            a field name against which to search for any associated Datatypes
     * @return a set of strings of associated Datatypes
     */
    public Set<String> getDataTypesForField(String fieldName) {
        if (fieldName == null || fieldName.isEmpty()) {
            return Collections.emptySet();
        }
        
        Set<String> dataTypes = new HashSet<>();
        for (Entry<String,Multimap<String,String>> entry : this.typeMetadata.entrySet()) {
            if (entry.getValue().containsKey(fieldName)) {
                dataTypes.add(entry.getKey());
            }
        }
        
        return dataTypes;
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
        typeMetadata.ingestTypes.addAll(datatypeFilter);
        typeMetadata.typeMetadata.putAll(localMap);
        return typeMetadata;
    }
    
    public boolean isEmpty() {
        return this.keySet().isEmpty();
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
    
    private static Map<String,Integer> parseTypes(String typeEntry) {
        // dts:[0:ingest1,1:ingest2]
        // types:[0:DateType,1:IntegerType,2:LcType]
        
        // remove type designation and leading/trailing brackets
        String types = typeEntry.split(":\\[")[1];
        String typeEntries = types.substring(0, types.length() - 1);
        
        Map<String,Integer> typeMap = new TreeMap<>();
        
        for (String entry : typeEntries.split(",")) {
            String[] entryParts = entry.split(":");
            typeMap.put(entryParts[1], Integer.valueOf(entryParts[0]));
        }
        
        return typeMap;
    }
    
    public String toString() {
        StringBuilder sb = new StringBuilder();
        
        // create and append ingestTypes mini-map
        sb.append("dts:[");
        Iterator<String> ingestIter = ingestTypes.iterator();
        for (int i = 0; i < ingestTypes.size(); i++) {
            String ingestType = ingestIter.next();
            sb.append(i).append(":");
            sb.append(ingestType);
            sb.append(ingestIter.hasNext() ? "," : "];");
            getIngestTypesMiniMap().put(ingestType, i);
        }
        
        // create and append dataTypes mini-map
        sb.append("types:[");
        Iterator<Multimap<String,String>> typesIter = typeMetadata.values().iterator();
        Set<String> dataTypes = new TreeSet<>();
        while (typesIter.hasNext()) {
            dataTypes.addAll(typesIter.next().values());
        }
        
        Iterator<String> dataIter = dataTypes.iterator();
        for (int i = 0; i < dataTypes.size(); i++) {
            String dataType = dataIter.next();
            sb.append(i).append(":");
            sb.append(dataType);
            sb.append(dataIter.hasNext() ? "," : "];");
            getDataTypesMiniMap().put(dataType, i);
        }
        
        // append fieldNames and their associated ingestTypes and Normalizers
        // ensure ordering for ease of type -> mini-map mapping
        Set<String> fieldNames = new TreeSet<>();
        Set<String> ingestTypes = typeMetadata.keySet().stream().sorted().collect(Collectors.toCollection(LinkedHashSet::new));
        for (String ingestType : ingestTypes) {
            fieldNames.addAll(typeMetadata.get(ingestType).keySet());
        }
        
        Iterator<String> fieldIter = fieldNames.iterator();
        while (fieldIter.hasNext()) {
            String fieldName = fieldIter.next();
            sb.append(fieldName).append(":[");
            Iterator<String> iIter = ingestTypes.iterator();
            while (iIter.hasNext()) {
                String ingestType = iIter.next();
                if (!typeMetadata.get(ingestType).containsKey(fieldName)) {
                    continue;
                }
                Iterator<String> dataTypeIter = typeMetadata.get(ingestType).get(fieldName).iterator();
                while (dataTypeIter.hasNext()) {
                    String dataType = dataTypeIter.next();
                    sb.append(getIngestTypesMiniMap().get(ingestType)).append(':');
                    sb.append(getDataTypesMiniMap().get(dataType));
                    sb.append(dataTypeIter.hasNext() ? "," : "");
                }
                sb.append(iIter.hasNext() ? "," : "");
            }
            sb.append(fieldIter.hasNext() ? "];" : "]");
        }
        
        return sb.toString();
    }
    
    private void fromString(String data) {
        String[] entries = parse(data, ';');
        
        if (entries.length > 2) {
            for (String entry : entries) {
                if (entry.startsWith(INGESTTYPE_PREFIX)) {
                    setIngestTypesMiniMap(parseTypes(entry));
                } else if (entry.startsWith(DATATYPES_PREFIX)) {
                    setDataTypesMiniMap(parseTypes(entry));
                } else {
                    String[] entrySplits = parse(entry, ':');
                    
                    // get rid of the leading and trailing brackets:
                    entrySplits[1] = entrySplits[1].substring(1, entrySplits[1].length() - 1);
                    String[] values = parse(entrySplits[1], ',');
                    
                    for (String aValue : values) {
                        if (!aValue.isEmpty()) { // ignore last entry for trailing comma
                            // @formatter:off
                        String[] vs = Iterables
                                .toArray(Splitter.on(':')
                                        .omitEmptyStrings()
                                        .trimResults()
                                        .split(aValue), String.class);

                        String ingestType = ImmutableMap.copyOf(getIngestTypesMiniMap())
                                .entrySet()
                                .stream()
                                .filter(e -> e.getValue().equals(Integer.valueOf(vs[0])))
                                .map(Entry::getKey)
                                .findFirst().orElse("");

                        String dataType = ImmutableMap.copyOf(getDataTypesMiniMap())
                                .entrySet()
                                .stream()
                                .filter(e -> e.getValue().equals(Integer.valueOf(vs[1])))
                                .map(Entry::getKey)
                                .findFirst().orElse("");
                        // @formatter:on
                            
                            this.addTypeMetadata(entrySplits[0], ingestType, dataType);
                        }
                    }
                    fieldNames.add(entrySplits[0]);
                }
            }
        }
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
            return other.typeMetadata == null;
        } else
            return toString().equals(obj.toString());
    }
    
    private void writeObject(ObjectOutputStream out) throws Exception {
        out.writeObject(this.toString());
    }
    
    private void readObject(ObjectInputStream in) throws Exception {
        this.ingestTypes = Sets.newTreeSet();
        this.fieldNames = Sets.newTreeSet();
        this.typeMetadata = Maps.newHashMap();
        this.fromString((String) in.readObject());
    }
    
    public static final TypeMetadata EMPTY_TYPE_METADATA = new EmptyTypeMetadata();
    
    public static TypeMetadata emptyTypeMetadata() {
        return EMPTY_TYPE_METADATA;
    }
    
    private static class EmptyTypeMetadata extends TypeMetadata implements Serializable {
        
        final Multimap<String,String> EMPTY_MULTIMAP = new ImmutableMultimap.Builder().build();
        
        @Override
        public Collection<String> getTypeMetadata(String fieldName, String ingestType) {
            return Collections.emptySet();
        }
        
        /**
         * returns a multimap of field name to datatype name ingest type names are not included
         *
         * @return
         */
        @Override
        public Multimap<String,String> fold() {
            return EMPTY_MULTIMAP;
        }
        
        /**
         * returns a multimap of field name to datatype name, filtered on provided ingest type names ingest type names are not included
         *
         * @param ingestTypeFilter
         * @return
         */
        @Override
        public Multimap<String,String> fold(Set<String> ingestTypeFilter) {
            return EMPTY_MULTIMAP;
        }
        
        public Set<Entry<String,Multimap<String,String>>> entrySet() {
            return Collections.emptySet();
        }
        
        @Override
        public Set<String> keySet() {
            return Collections.emptySet();
        }
        
        @Override
        public TypeMetadata filter(Set<String> datatypeFilter) {
            return this;
        }
        
        @Override
        public boolean equals(Object o) {
            return (o instanceof TypeMetadata) && ((TypeMetadata) o).isEmpty();
        }
        
        @Override
        public int hashCode() {
            return 0;
        }
        
        // Preserves singleton property
        private Object readResolve() {
            return EMPTY_TYPE_METADATA;
        }
    }
}

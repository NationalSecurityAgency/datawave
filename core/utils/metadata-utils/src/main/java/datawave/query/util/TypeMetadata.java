package datawave.query.util;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
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

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.base.Splitter;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

public class TypeMetadata implements Serializable, KryoSerializable {

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
     * @throws IllegalStateException
     *             if an ingest type is absent, rather than fold it away and normalize its values wrong
     */
    public Multimap<String,String> fold(Set<String> ingestTypeFilter) {
        if (ingestTypeFilter == null || ingestTypeFilter.isEmpty()) {
            return this.fold();
        }
        Multimap<String,String> map = HashMultimap.create();

        for (String type : ingestTypeFilter) {
            Multimap<String,String> types = this.typeMetadata.get(type);
            if (types == null) {
                throw new IllegalStateException("No TypeMetadata for ingest type '" + type + "'. Known ingest types: " + this.typeMetadata.keySet()
                                + ". The metadata table cache is likely incomplete; proceeding would normalize values against the wrong types and silently drop valid results.");
            }
            // defensive copy
            map.putAll(HashMultimap.create(types));
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

    /**
     * Parses one of the index to name mini-maps.
     *
     * @param typeEntry
     *            a {@code dts:[...]} or {@code types:[...]} entry
     * @return the mini-map, empty only when the entry holds no types
     * @throws IllegalStateException
     *             if the entry is malformed, rather than return a mini-map that would resolve field entries to the wrong types
     */
    private static Map<String,Integer> parseTypes(String typeEntry) {
        // dts:[0:ingest1,1:ingest2]
        // types:[0:DateType,1:IntegerType,2:LcType]

        // remove type designation and leading/trailing brackets
        String[] split = typeEntry.split(":\\[");
        if (split.length < 2) {
            throw new IllegalStateException("Malformed TypeMetadata mini-map, expected a bracketed list: '" + typeEntry + "'");
        }

        String types = split[1];
        if (!types.endsWith("]")) {
            throw new IllegalStateException("Malformed TypeMetadata mini-map, expected a closing bracket: '" + typeEntry + "'");
        }

        String typeEntries = types.substring(0, types.length() - 1);

        Map<String,Integer> typeMap = new TreeMap<>();
        if (typeEntries.isEmpty()) {
            // a legitimately empty list, not a malformed one
            return typeMap;
        }

        for (String entry : typeEntries.split(",")) {
            String[] entryParts = entry.split(":");
            if (entryParts.length < 2) {
                throw new IllegalStateException("Malformed TypeMetadata mini-map entry, expected 'index:name': '" + entry + "' in '" + typeEntry + "'");
            }
            typeMap.put(entryParts[1], Integer.valueOf(entryParts[0]));
        }

        return typeMap;
    }

    /**
     * Renders this instance in the form described by {@link #fromString(String)}. The ingest type and normalizer type mini-maps are built locally and published
     * at the end, because a cached instance is serialized concurrently by every query sharing its auths and datatype filter.
     *
     * @return a serialized TypeMetadata
     */
    public String toString() {
        StringBuilder sb = new StringBuilder();
        Map<String,Integer> ingestTypesMini = new TreeMap<>();
        Map<String,Integer> dataTypesMini = new TreeMap<>();

        // create and append ingestTypes mini-map
        sb.append("dts:[");
        Iterator<String> ingestIter = ingestTypes.iterator();
        for (int i = 0; i < ingestTypes.size(); i++) {
            String ingestType = ingestIter.next();
            sb.append(i).append(":");
            sb.append(ingestType);
            sb.append(ingestIter.hasNext() ? "," : "]");
            ingestTypesMini.put(ingestType, i);
        }
        // an empty TypeMetadata still has to close the bracket, or the result cannot be parsed back
        if (ingestTypes.isEmpty()) {
            sb.append("]");
        }
        sb.append(";");

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
            sb.append(dataIter.hasNext() ? "," : "]");
            dataTypesMini.put(dataType, i);
        }
        if (dataTypes.isEmpty()) {
            sb.append("]");
        }

        // append fieldNames and their associated ingestTypes and Normalizers
        // ensure ordering for ease of type -> mini-map mapping
        Set<String> fieldNames = new TreeSet<>();
        Set<String> ingestTypes = typeMetadata.keySet().stream().sorted().collect(Collectors.toCollection(LinkedHashSet::new));
        for (String ingestType : ingestTypes) {
            fieldNames.addAll(typeMetadata.get(ingestType).keySet());
        }

        Iterator<String> fieldIter = fieldNames.iterator();
        if (fieldIter.hasNext()) {
            sb.append(";");
        }
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
                    sb.append(ingestTypesMini.get(ingestType)).append(':');
                    sb.append(dataTypesMini.get(dataType));
                    sb.append(dataTypeIter.hasNext() ? "," : "");
                }
                sb.append(iIter.hasNext() ? "," : "");
            }
            sb.append(fieldIter.hasNext() ? "];" : "]");
        }

        // publish the completed maps as a single reference store, so a concurrent reader never observes a half-built map
        setIngestTypesMiniMap(ingestTypesMini);
        setDataTypesMiniMap(dataTypesMini);

        return sb.toString();
    }

    /**
     * Populates this instance from the semicolon delimited form produced by {@link #toString()}: a {@code dts} mini-map assigning an index to each ingest type,
     * a {@code types} mini-map assigning an index to each normalizer type held against a field, then one entry per field name whose bracketed list holds
     * {@code ingestTypeIndex:normalizerTypeIndex} pairs.
     *
     * <pre>
     * dts:[0:ingestA,1:ingestB];types:[0:LcNoDiacriticsType,1:NumberType];FIELD1:[0:0,1:0];FIELD2:[0:0,0:1,1:1]
     * </pre>
     *
     * The example above gives FIELD1 an LcNoDiacriticsType in both ingest types, and FIELD2 both types in ingestA and a NumberType in ingestB. Parsing is
     * lenient about what {@link #toString()} emits: input with fewer than three entries is discarded, empty entries and values are skipped (a field absent from
     * a trailing ingest type emits a trailing comma), and an index with no mini-map entry yields an empty type name. A malformed entry throws instead of
     * resolving field entries to the wrong types.
     *
     * @param data
     *            a serialized TypeMetadata
     * @throws IllegalStateException
     *             if a mini-map or field entry is malformed
     */
    private void fromString(String data) {
        String[] entries = parse(data, ';');

        if (entries.length > 2) {
            // invert each index -> name mini-map once, as its prefix entry is parsed, so that every field entry below
            // is an O(1) lookup instead of a linear scan per value. These start empty rather than inverting the
            // current mini-maps: those are unpopulated for a freshly parsed instance, and are still null when called
            // from readObject, which does not run a constructor.
            Map<Integer,String> ingestTypesByIndex = Collections.emptyMap();
            Map<Integer,String> dataTypesByIndex = Collections.emptyMap();

            for (String entry : entries) {
                if (entry.startsWith(INGESTTYPE_PREFIX)) {
                    setIngestTypesMiniMap(parseTypes(entry));
                    ingestTypesByIndex = invert(getIngestTypesMiniMap());
                } else if (entry.startsWith(DATATYPES_PREFIX)) {
                    setDataTypesMiniMap(parseTypes(entry));
                    dataTypesByIndex = invert(getDataTypesMiniMap());
                } else if (!entry.isEmpty()) {
                    // a field entry, FIELD_NAME:[ingestTypeIndex:normalizerTypeIndex,...], splitting into the field
                    // name and the bracketed list of index pairs. An empty entry is a trailing delimiter, not a field.
                    String[] entrySplits = parse(entry, ':');
                    if (entrySplits.length < 2) {
                        throw new IllegalStateException("Malformed TypeMetadata field entry, expected 'FIELD:[...]': '" + entry + "'");
                    }

                    // get rid of the leading and trailing brackets:
                    String indexPairs = entrySplits[1];
                    if (!indexPairs.startsWith("[") || !indexPairs.endsWith("]")) {
                        throw new IllegalStateException("Malformed TypeMetadata field entry, expected a bracketed list: '" + entry + "'");
                    }

                    entrySplits[1] = indexPairs.substring(1, indexPairs.length() - 1);
                    String[] values = parse(entrySplits[1], ',');

                    for (String aValue : values) {
                        if (!aValue.isEmpty()) { // ignore last entry for trailing comma
                            // an index pair, vs[0] into the ingest type mini-map and vs[1] into the normalizer type
                            // mini-map. An index the corresponding mini-map does not hold, including any index seen
                            // before its mini-map entry was parsed, resolves to "".
                            String[] vs = Iterables.toArray(Splitter.on(':').omitEmptyStrings().trimResults().split(aValue), String.class);
                            if (vs.length < 2) {
                                throw new IllegalStateException("Malformed TypeMetadata index pair, expected 'ingestTypeIndex:normalizerTypeIndex': '" + aValue
                                                + "' in '" + entry + "'");
                            }

                            String ingestType = ingestTypesByIndex.getOrDefault(Integer.valueOf(vs[0]), "");
                            String dataType = dataTypesByIndex.getOrDefault(Integer.valueOf(vs[1]), "");

                            this.addTypeMetadata(entrySplits[0], ingestType, dataType);
                        }
                    }
                    fieldNames.add(entrySplits[0]);
                }
            }
        }
    }

    private static Map<Integer,String> invert(Map<String,Integer> nameToIndex) {
        Map<Integer,String> byIndex = Maps.newHashMapWithExpectedSize(nameToIndex.size());
        for (Entry<String,Integer> entry : nameToIndex.entrySet()) {
            byIndex.put(entry.getValue(), entry.getKey());
        }
        return byIndex;
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
        // deserialization does not run a constructor, so the mini-maps must be initialized here too
        this.ingestTypesMiniMap = new TreeMap<>();
        this.dataTypesMiniMap = new TreeMap<>();
        this.fromString((String) in.readObject());
    }

    /**
     * Writes this instance in the binary equivalent of the form described by {@link #fromString(String)}: an ingest type mini-map, a normalizer type mini-map,
     * then one entry per field name holding its {@code ingestTypeIndex:normalizerTypeIndex} pairs as variable length ints. Each name is written exactly once,
     * which matters because a normalizer type name is otherwise repeated for every field it is held against. The mini-maps are populated as a side effect, as
     * they are by {@link #toString()}.
     * <p>
     * Unlike {@link #writeObject(ObjectOutputStream)} this bypasses {@link #toString()}, building the mini-maps against the underlying {@code typeMetadata} map
     * rather than through a {@link StringBuilder}.
     *
     * @param kryo
     *            the kryo instance
     * @param output
     *            the kryo output
     */
    @Override
    public void write(Kryo kryo, Output output) {
        // the ingest types actually referenced below, sorted so the assigned indices are stable across instances
        Set<String> ingestTypes = new TreeSet<>(typeMetadata.keySet());
        Map<String,Integer> ingestTypeIndices = new TreeMap<>();
        output.writeInt(ingestTypes.size(), true);
        for (String ingestType : ingestTypes) {
            output.writeString(ingestType);
            ingestTypeIndices.put(ingestType, ingestTypeIndices.size());
        }
        setIngestTypesMiniMap(ingestTypeIndices);

        Set<String> dataTypes = new TreeSet<>();
        for (Multimap<String,String> fieldMap : typeMetadata.values()) {
            dataTypes.addAll(fieldMap.values());
        }
        Map<String,Integer> dataTypeIndices = new TreeMap<>();
        output.writeInt(dataTypes.size(), true);
        for (String dataType : dataTypes) {
            output.writeString(dataType);
            dataTypeIndices.put(dataType, dataTypeIndices.size());
        }
        setDataTypesMiniMap(dataTypeIndices);

        // invert the ingest type major typeMetadata into the field major order written below, in a single pass over
        // its entries rather than a lookup per field per ingest type. Field names come from typeMetadata rather than
        // the fieldNames member because filter() populates the former without the latter.
        Map<String,List<Integer>> pairsByField = new TreeMap<>();
        for (Entry<String,Multimap<String,String>> ingestEntry : typeMetadata.entrySet()) {
            Integer ingestTypeIndex = ingestTypeIndices.get(ingestEntry.getKey());
            for (Entry<String,Collection<String>> fieldEntry : ingestEntry.getValue().asMap().entrySet()) {
                List<Integer> pairs = pairsByField.computeIfAbsent(fieldEntry.getKey(), field -> new ArrayList<>());
                for (String dataType : fieldEntry.getValue()) {
                    pairs.add(ingestTypeIndex);
                    pairs.add(dataTypeIndices.get(dataType));
                }
            }
        }

        output.writeInt(pairsByField.size(), true);
        for (Entry<String,List<Integer>> fieldEntry : pairsByField.entrySet()) {
            output.writeString(fieldEntry.getKey());
            List<Integer> pairs = fieldEntry.getValue();
            output.writeInt(pairs.size() / 2, true);
            for (Integer index : pairs) {
                output.writeInt(index, true);
            }
        }
    }

    /**
     * Reads this instance from the form written by {@link #write(Kryo, Output)}, resolving each index pair against the two mini-maps held as arrays, so that
     * every field entry is an O(1) lookup rather than the repeated map lookups {@link #fromString(String)} performs.
     *
     * @param kryo
     *            the kryo instance
     * @param input
     *            the kryo input
     */
    @Override
    public void read(Kryo kryo, Input input) {
        this.ingestTypes = Sets.newTreeSet();
        this.fieldNames = Sets.newTreeSet();
        this.typeMetadata = Maps.newHashMap();
        this.ingestTypesMiniMap = new TreeMap<>();
        this.dataTypesMiniMap = new TreeMap<>();

        String[] ingestTypesByIndex = new String[input.readInt(true)];
        for (int i = 0; i < ingestTypesByIndex.length; i++) {
            ingestTypesByIndex[i] = input.readString();
            this.ingestTypesMiniMap.put(ingestTypesByIndex[i], i);
        }

        String[] dataTypesByIndex = new String[input.readInt(true)];
        for (int i = 0; i < dataTypesByIndex.length; i++) {
            dataTypesByIndex[i] = input.readString();
            this.dataTypesMiniMap.put(dataTypesByIndex[i], i);
        }

        int numFields = input.readInt(true);
        for (int i = 0; i < numFields; i++) {
            String fieldName = input.readString();
            int pairs = input.readInt(true);
            for (int j = 0; j < pairs; j++) {
                String ingestType = ingestTypesByIndex[input.readInt(true)];
                String dataType = dataTypesByIndex[input.readInt(true)];
                this.addTypeMetadata(fieldName, ingestType, dataType);
            }
            this.fieldNames.add(fieldName);
        }
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

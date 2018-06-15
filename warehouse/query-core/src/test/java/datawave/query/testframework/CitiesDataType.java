package datawave.query.testframework;

import datawave.data.type.NoOpType;
import datawave.data.type.NumberType;
import datawave.ingest.data.config.CSVHelper;
import datawave.ingest.data.config.ingest.BaseIngestHelper;
import datawave.ingest.data.config.ingest.CompositeIngest;
import datawave.ingest.data.config.ingest.VirtualIngest;
import datawave.ingest.input.reader.EventRecordReader;
import datawave.query.Constants;

import java.io.IOException;
import java.lang.reflect.Type;
import java.net.URISyntaxException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Contains all of the relevant data needed to configure any of the cities data types.
 */
public class CitiesDataType extends AbstractDataTypeConfig {
    
    private static final Random rVal = new Random(System.currentTimeMillis());
    
    /**
     * List of cities that are used for testing. Each enumeration will contain the path of the data ingest file.
     */
    public enum CityEntry {
        paris("input/paris-cities.csv", "paris"), london("input/london-cities.csv", "london"), rome("input/rome-cities.csv", "rome");
        
        private final String ingestFile;
        private final String cityName;
        
        CityEntry(final String file, final String name) {
            this.ingestFile = file;
            this.cityName = name;
        }
        
        private String getIngestFile() {
            return this.ingestFile;
        }
        
        /**
         * Returns a random city name.
         *
         * @return random city name
         */
        public static String getRandomCity() {
            final CityEntry[] cities = CityEntry.values();
            final int idx = rVal.nextInt(cities.length);
            return cities[idx].cityName;
        }
    }
    
    /**
     * Defines the valid dates that are used for shard ids. All test data should specify one of the shard id dates.
     */
    public enum CityShardId {
        DATE_2015_0707("20150707"),
        DATE_2015_0808("20150808"),
        DATE_2015_0909("20150909"),
        DATE_2016_0404("20160404"),
        DATE_2016_0505("20160505"),
        DATE_2016_0606("20160606");
        
        static Set<String> getShardRange(final Date start, final Date end) {
            final Set<String> shards = new HashSet<>();
            for (final CityShardId id : CityShardId.values()) {
                int s = start.compareTo(id.date);
                int e = end.compareTo(id.date);
                if (0 >= start.compareTo(id.date) && 0 <= end.compareTo(id.date)) {
                    shards.add(id.dateStr);
                }
            }
            
            return shards;
        }
        
        static final List<Date> sortedDate = new ArrayList<>();
        
        static Date[] generateRandomStartEndDates() {
            // use double check locking
            if (sortedDate.isEmpty()) {
                synchronized (sortedDate) {
                    if (sortedDate.isEmpty()) {
                        final List<Date> dates = new ArrayList<>();
                        for (final CityShardId id : CityShardId.values()) {
                            dates.add(id.date);
                        }
                        Collections.sort(dates);
                        sortedDate.addAll(dates);
                    }
                }
            }
            
            Date[] startEndDate = new Date[2];
            int s = rVal.nextInt(sortedDate.size());
            startEndDate[0] = sortedDate.get(s);
            int remaining = sortedDate.size() - s;
            startEndDate[1] = startEndDate[0];
            if (0 < remaining) {
                int e = rVal.nextInt(sortedDate.size() - s);
                startEndDate[1] = sortedDate.get(s + e);
            }
            
            return startEndDate;
        }
        
        private final String dateStr;
        private final Date date;
        
        CityShardId(final String str) {
            this.dateStr = str;
            try {
                this.date = YMD_DateFormat.parse(str);
            } catch (ParseException pe) {
                throw new AssertionError("invalid date string(" + str + ")");
            }
        }
        
        /**
         * Returns the accumulo shard id string representation.
         *
         * @return accumulo shard id
         */
        String getShardId() {
            return this.dateStr + "_0";
        }
        
        public Date date() {
            return this.date;
        }
        
        static Collection<String> cityShards() {
            return Stream.of(CityShardId.values()).map(e -> e.getShardId()).collect(Collectors.toList());
        }
    }
    
    /**
     * Defines the data fields for cities datatype.
     */
    public enum CityField {
        // order is important, should match the order in the csv files
        START_DATE(String.class),
        EVENT_ID(String.class),
        CITY(String.class),
        STATE(String.class, true),
        COUNTRY(String.class),
        CONTINENT(String.class),
        CODE(String.class),
        ACCESS(String.class),
        NUM((Integer.class));
        
        private static final Collection<List<String>> CompositeMapping = new HashSet<>();
        private static final Collection<List<String>> VirtualMapping = new HashSet<>();
        private static final String MultiValueFields;
        private static final List<String> Headers;
        
        static {
            CompositeMapping.add(Arrays.asList(CITY.name(), STATE.name()));
            CompositeMapping.add(Arrays.asList(CITY.name(), COUNTRY.name()));
            CompositeMapping.add(Arrays.asList(CITY.name(), NUM.name()));
            CompositeMapping.add(Arrays.asList(CITY.name(), CONTINENT.name()));
            CompositeMapping.add(Arrays.asList(CITY.name(), NUM.name()));
            
            VirtualMapping.add(Arrays.asList(STATE.name(), COUNTRY.name()));
            
            MultiValueFields = STATE.name();
            Headers = Stream.of(CityField.values()).map(e -> e.name()).collect(Collectors.toList());
        }
        
        /**
         * Retrieves the enumeration that matches the specified field.
         *
         * @param field
         *            string representation of the field as returned by {@link #name}
         * @return enumeration value
         * @throws AssertionError
         *             field does not match any of the enumeration values
         */
        public static CityField getField(final String field) {
            for (final CityField f : CityField.values()) {
                if (f.name().equalsIgnoreCase(field)) {
                    return f;
                }
            }
            
            throw new AssertionError("invalid city field(" + field + ")");
        }
        
        public static List<String> headers() {
            return Headers;
        }
        
        /**
         * Returns a random set of fields, with o without {@link #EVENT_ID}.
         *
         * @param withEventId
         *            when true, include the event id
         * @return set of random fields
         */
        public static Set<String> getRandomReturnFields(final boolean withEventId) {
            final Set<String> fields = new HashSet<>();
            for (final CityField field : CityField.values()) {
                if (rVal.nextBoolean()) {
                    fields.add(field.name());
                }
            }
            
            // check to see if event id must be included
            if (withEventId) {
                fields.add(CityField.EVENT_ID.name());
            } else {
                fields.remove(CityField.EVENT_ID.name());
            }
            
            return fields;
        }
        
        /**
         * Creates a mapping of field names to the type
         *
         * @return key/value mapping of key to type
         */
        static Map<String,Type> getFieldTypeMapping() {
            final Map<String,Type> mapping = new HashMap<>();
            for (final CityField field : CityField.values()) {
                mapping.put(field.name(), field.metadata.type);
            }
            return mapping;
        }
        
        static Type getFieldType(final String fieldName) {
            for (final CityField field : CityField.values()) {
                if (field.name().equalsIgnoreCase(fieldName)) {
                    return field.metadata.type;
                }
            }
            
            // check for ANY_FIELD
            if (Constants.ANY_FIELD.equals(fieldName)) {
                return String.class;
            }
            
            throw new AssertionError("invalid city field(" + fieldName + ")");
        }
        
        private static final Map<String,BaseRawData.RawMetaData> metadataMapping = new HashMap<>();
        
        static Map<String,BaseRawData.RawMetaData> getMetaDataMapping() {
            if (metadataMapping.isEmpty()) {
                synchronized (metadataMapping) {
                    if (metadataMapping.isEmpty()) {
                        for (final CityField field : CityField.values()) {
                            metadataMapping.put(field.name(), field.metadata);
                        }
                    }
                }
            }
            
            return metadataMapping;
        }
        
        /**
         * Returns all indexed field names, including composite indexes.
         *
         * @return list of all indexed fields
         */
        static List<String> indexFields() {
            final List<String> fields = compositeNames();
            fields.addAll(indexFieldsOnly());
            return fields;
        }
        
        /**
         * Returns only the indexed fields without the composite indexes.
         *
         * @return list of indexed fields
         */
        static List<String> indexFieldsOnly() {
            return Arrays.asList(CITY.name(), STATE.name(), COUNTRY.name(), CONTINENT.name(), CODE.name(), NUM.name());
        }
        
        static List<String> anyFieldIndex() {
            final List<String> fields = new ArrayList<>(indexFieldsOnly());
            fields.remove(NUM.name());
            return fields;
        }
        
        private static String multiValueFields() {
            return MultiValueFields;
        }
        
        private static List<String> compositeNames() {
            final List<String> names = new ArrayList<>();
            for (final List<String> composite : CompositeMapping) {
                names.add(String.join("_", composite));
            }
            
            return names;
        }
        
        private static List<String> virtualFields() {
            final List<String> fields = new ArrayList<>();
            for (final List<String> virtual : VirtualMapping) {
                fields.add(String.join(".", virtual));
            }
            return fields;
        }
        
        private static List<String> virtualNames() {
            final List<String> names = new ArrayList<>();
            for (final List<String> virtual : VirtualMapping) {
                names.add(String.join("_", virtual));
            }
            
            return names;
        }
        
        private static List<String> compositeFields() {
            final List<String> fields = new ArrayList<>();
            for (final List<String> composite : CompositeMapping) {
                fields.add(String.join(".", composite));
            }
            return fields;
        }
        
        private static Collection<String> reverseIndexFields() {
            return Arrays.asList(CITY.name(), STATE.name(), COUNTRY.name(), CONTINENT.name(), CODE.name());
        }
        
        private BaseRawData.RawMetaData metadata;
        
        CityField(final Type type) {
            this(type, false);
        }
        
        CityField(final Type type, final boolean isMulti) {
            this.metadata = new BaseRawData.RawMetaData(this.name(), type, isMulti);
        }
        
        /**
         * Returns the class associated with the field.
         *
         * @return class type
         */
        public Type getType() {
            return this.metadata.type;
        }
        
        /**
         * Returns the metadata for this field.
         *
         * @return metadata
         */
        public BaseRawData.RawMetaData getMetadata() {
            return metadata;
        }
    }
    
    // ==================================
    // pojo manager info
    private static final IRawDataManager cityManager = new CityPOJOManager();
    
    public static IRawDataManager getManager() {
        return cityManager;
    }
    
    /**
     * Creates a cities datatype entry with all of the key/value configuration settings.
     *
     * @param city
     *            city entry for ingest
     * @throws IOException
     *             unable to load ingest file
     * @throws URISyntaxException
     *             unable to resolve ingest file
     */
    public CitiesDataType(final CityEntry city) throws IOException, URISyntaxException {
        super(city.name(), city.getIngestFile(), cityManager);
        
        // NOTE: see super for default settings
        // set datatype settings
        this.hConf.set(this.dataType + "." + CityField.NUM + BaseIngestHelper.FIELD_TYPE, NumberType.class.getName());
        this.hConf.set(this.dataType + EventRecordReader.Properties.EVENT_DATE_FIELD_NAME, CityField.START_DATE.name());
        this.hConf.set(this.dataType + EventRecordReader.Properties.EVENT_DATE_FIELD_FORMAT, DATE_FIELD_FORMAT);
        
        this.hConf.set(this.dataType + ".data.category.id.field", CityField.EVENT_ID.name());
        
        // fields
        this.hConf.set(this.dataType + CSVHelper.DATA_HEADER, String.join(",", CityField.headers()));
        this.hConf.set(this.dataType + BaseIngestHelper.INDEX_FIELDS, String.join(",", CityField.indexFields()));
        this.hConf.set(this.dataType + BaseIngestHelper.REVERSE_INDEX_FIELDS, String.join(",", CityField.reverseIndexFields()));
        this.hConf.set(this.dataType + CompositeIngest.COMPOSITE_FIELD_NAMES, String.join(",", CityField.compositeNames()));
        this.hConf.set(this.dataType + CompositeIngest.COMPOSITE_FIELD_MEMBERS, String.join(",", CityField.compositeFields()));
        
        // type for composite fields
        for (final String composite : CityField.compositeNames()) {
            this.hConf.set(this.dataType + "." + composite, NoOpType.class.getName());
        }
        
        // virtual fields
        this.hConf.set(this.dataType + VirtualIngest.VIRTUAL_FIELD_NAMES, String.join(",", CityField.virtualNames()));
        this.hConf.set(this.dataType + VirtualIngest.VIRTUAL_FIELD_MEMBERS, String.join(",", CityField.virtualFields()));
        
        // multivalue fields
        this.hConf.set(this.dataType + CSVHelper.MULTI_VALUED_FIELDS, CityField.MultiValueFields);
    }
    
    @Override
    public Collection<String> getShardIds() {
        return CityShardId.cityShards();
    }
    
    @Override
    public String toString() {
        return "CitiesDataType{" + "dataType='" + dataType + '\'' + ", ingestPath=" + ingestPath + '}';
    }
}

package datawave.query.testframework;

import datawave.data.type.NumberType;
import datawave.ingest.data.config.CSVHelper;
import datawave.ingest.data.config.ingest.BaseIngestHelper;
import datawave.ingest.input.reader.EventRecordReader;
import datawave.query.Constants;

import java.io.IOException;
import java.lang.reflect.Type;
import java.net.URISyntaxException;
import java.text.ParseException;
import java.util.ArrayList;
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
        paris("input/paris-cities.csv", "paris"), london("input/london-cities.csv", "london"), rome("input/rome-cities.csv", "rome"), generic(
                        "input/generic-cities.csv", "generic"), multivalue("input/multivalue-cities.csv", "multi");
        
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
        DATE_2015_0707("20150707"), DATE_2015_0808("20150808"), DATE_2015_0909("20150909"), DATE_2015_1010("20151010"), DATE_2015_1111("20151111");
        
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
        
        static Date[] getStartEndDates(final boolean random) {
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
            if (random) {
                int s = rVal.nextInt(sortedDate.size());
                startEndDate[0] = sortedDate.get(s);
                int remaining = sortedDate.size() - s;
                startEndDate[1] = startEndDate[0];
                if (0 < remaining) {
                    int e = rVal.nextInt(sortedDate.size() - s);
                    startEndDate[1] = sortedDate.get(s + e);
                }
            } else {
                int s = rVal.nextInt(sortedDate.size());
                startEndDate[0] = sortedDate.get(0);
                startEndDate[1] = sortedDate.get(sortedDate.size() - 1);
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
        CITY(String.class, true),
        STATE(String.class, true),
        COUNTRY(String.class),
        CONTINENT(String.class),
        CODE(String.class),
        ACCESS(String.class),
        NUM((Integer.class));
        
        private static final List<String> Headers;
        
        static {
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
    // data manager info
    private static final IRawDataManager cityManager = new CityDataManager();
    
    public static IRawDataManager getManager() {
        return cityManager;
    }
    
    /**
     * Creates a cities datatype entry with all of the key/value configuration settings.
     *
     * @param city
     *            city entry for ingest
     * @param config
     *            hadoop field configuration
     * @throws IOException
     *             unable to load ingest file
     * @throws URISyntaxException
     *             unable to resolve ingest file
     */
    public CitiesDataType(final CityEntry city, final IFieldConfig config) throws IOException, URISyntaxException {
        super(city.name(), city.getIngestFile(), config, cityManager);
        
        // NOTE: see super for default settings
        // set datatype settings
        this.hConf.set(this.dataType + "." + CityField.NUM + BaseIngestHelper.FIELD_TYPE, NumberType.class.getName());
        this.hConf.set(this.dataType + EventRecordReader.Properties.EVENT_DATE_FIELD_NAME, CityField.START_DATE.name());
        this.hConf.set(this.dataType + EventRecordReader.Properties.EVENT_DATE_FIELD_FORMAT, DATE_FIELD_FORMAT);
        
        this.hConf.set(this.dataType + ".data.category.id.field", CityField.EVENT_ID.name());
        
        // fields
        this.hConf.set(this.dataType + CSVHelper.DATA_HEADER, String.join(",", CityField.headers()));
    }
    
    @Override
    public Collection<String> getShardIds() {
        return CityShardId.cityShards();
    }
    
    @Override
    public String toString() {
        return "CitiesDataType{" + super.toString() + "}";
    }
}

package datawave.query.testframework;

import datawave.data.normalizer.Normalizer;
import datawave.data.type.LcNoDiacriticsType;
import datawave.data.type.NumberType;
import datawave.ingest.csv.config.helper.ExtendedCSVHelper;
import datawave.ingest.data.config.CSVHelper;
import datawave.ingest.data.config.ingest.BaseIngestHelper;
import datawave.ingest.input.reader.EventRecordReader;
import org.apache.log4j.Logger;

import java.io.IOException;
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
    
    private static final Logger log = Logger.getLogger(CitiesDataType.class);
    private static final Random rVal = new Random(System.currentTimeMillis());
    
    /**
     * List of cities that are used for testing. Each enumeration will contain the path of the data ingest file.
     */
    public enum CityEntry {
        // default provided cities with datatype name
        paris("input/paris-cities.csv", "paris"),
        london("input/london-cities.csv", "london"),
        rome("input/rome-cities.csv", "rome"),
        usa("input/usa-cities.csv", "usa"),
        dup_usa("input/usa-cities-dup.csv", "dup-usa"),
        italy("input/italy-cities.csv", "italy"),
        // set of generic entries for london, paris, and rome
        generic("input/generic-cities.csv", "generic"),
        // contains null values for state entries
        nullState("input/null-city.csv", "null"),
        // used to create a index hole when used in conjunction with generic
        hole("input/index-hole.csv", "hole"),
        // contains multivalue entries for city and state
        multivalue("input/multivalue-cities.csv", "multi");
        
        private final String ingestFile;
        private final String cityName; // also serves as datatype
        
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
        
        /**
         * Returns the datatype for the entry.
         * 
         * @return datatype for instance
         */
        public String getDataType() {
            return this.cityName;
        }
    }
    
    /**
     * Defines the valid dates that are used for shard ids. All test data should specify one of the shard id dates.
     */
    public enum CityShardId {
        // list of shards for testing
        DATE_2015_0404("20150404"),
        DATE_2015_0505("20150505"),
        DATE_2015_0606("20150606"),
        DATE_2015_0707("20150707"),
        DATE_2015_0808("20150808"),
        DATE_2015_0909("20150909"),
        DATE_2015_1010("20151010"),
        DATE_2015_1111("20151111");
        
        static Set<String> getShardRange(final Date start, final Date end) {
            final Set<String> shards = new HashSet<>();
            for (final CityShardId id : CityShardId.values()) {
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
        
        public Date getDate() {
            return this.date;
        }
        
        public String getDateStr() {
            return this.dateStr;
        }
    }
    
    /**
     * Defines the data fields for cities datatype.
     */
    public enum CityField {
        // order is important, should match the order in the csv files
        START_DATE(Normalizer.LC_NO_DIACRITICS_NORMALIZER),
        EVENT_ID(Normalizer.LC_NO_DIACRITICS_NORMALIZER),
        CITY(Normalizer.LC_NO_DIACRITICS_NORMALIZER, true),
        STATE(Normalizer.LC_NO_DIACRITICS_NORMALIZER, true),
        COUNTRY(Normalizer.LC_NO_DIACRITICS_NORMALIZER),
        CONTINENT(Normalizer.LC_NO_DIACRITICS_NORMALIZER),
        CODE(Normalizer.LC_NO_DIACRITICS_NORMALIZER),
        ACCESS(Normalizer.LC_NO_DIACRITICS_NORMALIZER),
        NUM((Normalizer.NUMBER_NORMALIZER));
        
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
        
        private static final Map<String,BaseRawData.RawMetaData> metadataMapping = new HashMap<>();
        
        private BaseRawData.RawMetaData metadata;
        
        CityField(final Normalizer<?> normalizer) {
            this(normalizer, false);
        }
        
        CityField(final Normalizer<?> normalizer, final boolean isMulti) {
            this.metadata = new BaseRawData.RawMetaData(this.name(), normalizer, isMulti);
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
    private static final RawDataManager cityManager = new CityDataManager();
    
    public static RawDataManager getManager() {
        return cityManager;
    }
    
    /**
     * Creates a cities datatype entry with all of the key/value configuration settings.
     *
     * @param city
     *            entry for ingest containing datatype and ingest file
     * @param config
     *            hadoop field configuration
     * @throws IOException
     *             unable to load ingest file
     * @throws URISyntaxException
     *             unable to resolve ingest file
     */
    public CitiesDataType(final CityEntry city, final FieldConfig config) throws IOException, URISyntaxException {
        this(city.getDataType(), city.getIngestFile(), config);
    }
    
    /**
     * Constructor for city/ingest files that are not defined in the class {@link CityEntry}.
     * 
     * @param city
     *            name of the city datatype
     * @param ingestFile
     *            ingest file path
     * @param config
     *            hadoop field configuration
     * @throws IOException
     * @throws URISyntaxException
     */
    public CitiesDataType(final String city, final String ingestFile, final FieldConfig config) throws IOException, URISyntaxException {
        super(city, ingestFile, config, cityManager);
        
        // NOTE: see super for default settings
        // set datatype settings
        this.hConf.set(this.dataType + "." + CityField.NUM.name() + BaseIngestHelper.FIELD_TYPE, NumberType.class.getName());
        this.hConf.set(this.dataType + EventRecordReader.Properties.EVENT_DATE_FIELD_NAME, CityField.START_DATE.name());
        this.hConf.set(this.dataType + EventRecordReader.Properties.EVENT_DATE_FIELD_FORMAT, DATE_FIELD_FORMAT);
        
        this.hConf.set(this.dataType + ExtendedCSVHelper.Properties.EVENT_ID_FIELD_NAME, CityField.EVENT_ID.name());
        
        // fields
        this.hConf.set(this.dataType + CSVHelper.DATA_HEADER, String.join(",", CityField.headers()));
        
        // the CODE field type needs to be set for the index hole tests
        this.hConf.set(this.dataType + "." + CityField.CODE.name() + BaseIngestHelper.FIELD_TYPE, LcNoDiacriticsType.class.getName());
        
        log.debug(this.toString());
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

package datawave.query.testframework;

import datawave.data.normalizer.Normalizer;
import datawave.data.type.LcNoDiacriticsType;
import datawave.data.type.NumberType;
import datawave.ingest.csv.config.helper.ExtendedCSVHelper;
import datawave.ingest.data.config.CSVHelper;
import datawave.ingest.data.config.ingest.BaseIngestHelper;
import datawave.ingest.input.reader.EventRecordReader;
import datawave.marking.MarkingFunctions;

import org.apache.accumulo.core.security.Authorizations;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.URISyntaxException;
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
     * Contains predefined names for the cities datatype. Each enumeration will contain the path of the data ingest file.
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
        multivalue("input/multivalue-cities.csv", "multi"),
        // values for max expansion tests
        maxExp("input/max-expansion-cities.csv", "max-exp");
        
        private final String ingestFile;
        private final String cityName; // also serves as datatype
        
        CityEntry(final String file, final String name) {
            this.ingestFile = file;
            this.cityName = name;
        }
        
        public String getIngestFile() {
            return this.ingestFile;
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
        NUM((Normalizer.NUMBER_NORMALIZER)),
        GEO(Normalizer.GEO_NORMALIZER);
        
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
        
        private static final Map<String,RawMetaData> fieldMetadata;
        static {
            fieldMetadata = new HashMap<>();
            for (CityField field : CityField.values()) {
                fieldMetadata.put(field.name().toLowerCase(), field.metadata);
            }
        }
        
        /**
         * Returns mapping of ip address fields to the metadata for the field.
         * 
         * @return populate map
         */
        public static Map<String,RawMetaData> getFieldsMetadata() {
            return fieldMetadata;
        }
        
        /**
         * Returns a random set of fields, with or without {@link #EVENT_ID}.
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
        
        private static final Map<String,RawMetaData> metadataMapping = new HashMap<>();
        
        private RawMetaData metadata;
        
        CityField(final Normalizer<?> normalizer) {
            this(normalizer, false);
        }
        
        CityField(final Normalizer<?> normalizer, final boolean isMulti) {
            this.metadata = new RawMetaData(this.name(), normalizer, isMulti);
        }
        
        /**
         * Returns the metadata for this field.
         *
         * @return metadata
         */
        public RawMetaData getMetadata() {
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
     *             error loading test data
     * @throws URISyntaxException
     *             invalid test data file
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
    
    private static final String[] AUTH_VALUES = new String[] {"Euro", "NA"};
    private static final Authorizations TEST_AUTHS = new Authorizations(AUTH_VALUES);
    private static final Authorizations EXPANSION_AUTHS = new Authorizations("ct-a", "b-ct", "not-b-ct");
    
    public static Authorizations getTestAuths() {
        return TEST_AUTHS;
    }
    
    public static Authorizations getExpansionAuths() {
        return EXPANSION_AUTHS;
    }
    
    @Override
    public String getSecurityMarkingFieldNames() {
        return CityField.ACCESS.name();
    }
    
    @Override
    public String getSecurityMarkingFieldDomains() {
        return MarkingFunctions.Default.COLUMN_VISIBILITY;
    }
    
    @Override
    public String toString() {
        return this.getClass().getSimpleName() + "{" + super.toString() + "}";
    }
}

package datawave.query.testframework.cardata;

import datawave.data.normalizer.Normalizer;
import datawave.data.type.NumberType;
import datawave.ingest.data.config.CSVHelper;
import datawave.ingest.data.config.ingest.BaseIngestHelper;
import datawave.ingest.input.reader.EventRecordReader;
import datawave.query.testframework.AbstractDataTypeConfig;
import datawave.query.testframework.FieldConfig;
import datawave.query.testframework.RawDataManager;
import datawave.query.testframework.RawMetaData;
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
 * Contains all of the relevant data needed to configure any of the cars data types.
 */
public class CarsDataType extends AbstractDataTypeConfig {
    
    private static final Logger log = Logger.getLogger(CarsDataType.class);
    private static final Random rVal = new Random(System.currentTimeMillis());
    
    /**
     * List of cars that are used for testing. Each enumeration will contain the path of the data ingest file.
     */
    public enum CarEntry {
        // default provided cars with datatype name
        tesla("input/tesla-cars.csv", "tesla"),
        ford("input/ford-cars.csv", "tesla");
        
        private final String ingestFile;
        private final String carName;
        
        CarEntry(final String file, final String name) {
            this.ingestFile = file;
            this.carName = name;
        }
        
        private String getIngestFile() {
            return this.ingestFile;
        }
        
        /**
         * Returns a random car name.
         *
         * @return random car name
         */
        public static String getRandomCar() {
            final CarEntry[] cars = CarEntry.values();
            final int idx = rVal.nextInt(cars.length);
            return cars[idx].carName;
        }
    }
    
    /**
     * Defines the data fields for car datatype.
     */
    public enum CarField {
        // order is important, should match the order in the csv files
        START_DATE(Normalizer.LC_NO_DIACRITICS_NORMALIZER),
        EVENT_ID(Normalizer.LC_NO_DIACRITICS_NORMALIZER),
        MAKE(Normalizer.LC_NO_DIACRITICS_NORMALIZER, true),
        MODEL(Normalizer.LC_NO_DIACRITICS_NORMALIZER, true),
        COLOR(Normalizer.LC_NO_DIACRITICS_NORMALIZER, true),
        DOORS(Normalizer.NUMBER_NORMALIZER),
        WHEELS(Normalizer.NUMBER_NORMALIZER),
        DESC(Normalizer.LC_NO_DIACRITICS_NORMALIZER, true);
        
        private static final List<String> Headers;
        
        static {
            Headers = Stream.of(CarField.values()).map(e -> e.name()).collect(Collectors.toList());
        }
        
        private static final Map<String,RawMetaData> fieldMetadata;
        static {
            fieldMetadata = new HashMap<>();
            for (CarField field : CarField.values()) {
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
         * Retrieves the enumeration that matches the specified field.
         *
         * @param field
         *            string representation of the field as returned by {@link #name}
         * @return enumeration value
         * @throws AssertionError
         *             field does not match any of the enumeration values
         */
        public static CarField getField(final String field) {
            for (final CarField f : CarField.values()) {
                if (f.name().equalsIgnoreCase(field)) {
                    return f;
                }
            }
            
            throw new AssertionError("invalid car field(" + field + ")");
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
            for (final CarField field : CarField.values()) {
                if (rVal.nextBoolean()) {
                    fields.add(field.name());
                }
            }
            
            // check to see if event id must be included
            if (withEventId) {
                fields.add(CarField.EVENT_ID.name());
            } else {
                fields.remove(CarField.EVENT_ID.name());
            }
            
            return fields;
        }
        
        private static final Map<String,RawMetaData> metadataMapping = new HashMap<>();
        
        private RawMetaData metadata;
        
        CarField(final Normalizer<?> normalizer) {
            this(normalizer, false);
        }
        
        CarField(final Normalizer<?> normalizer, final boolean isMulti) {
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
    private static final RawDataManager carManager = new CarDataManager();
    
    public static RawDataManager getManager() {
        return carManager;
    }
    
    /**
     * Creates a cars datatype entry with all of the key/value configuration settings.
     *
     * @param car
     *            entry for ingest containing datatype and ingest file
     * @param config
     *            hadoop field configuration
     * @throws IOException
     *             unable to load ingest file
     * @throws URISyntaxException
     *             unable to resolve ingest file
     */
    public CarsDataType(final CarEntry car, final FieldConfig config) throws IOException, URISyntaxException {
        this(car.name(), car.getIngestFile(), config);
    }
    
    /**
     * Constructor for car/ingest files that are not defined in the class {@link CarEntry}.
     *
     * @param car
     *            name of the car datatype
     * @param ingestFile
     *            ingest file path
     * @param config
     *            hadoop field configuration
     * @throws IOException
     * @throws URISyntaxException
     */
    public CarsDataType(final String car, final String ingestFile, final FieldConfig config) throws IOException, URISyntaxException {
        super(car, ingestFile, config, carManager);
        
        // NOTE: see super for default settings
        // set datatype settings
        this.hConf.set(this.dataType + "." + CarField.DOORS + BaseIngestHelper.FIELD_TYPE, NumberType.class.getName());
        // this.hConf.set(this.dataType + "." + CarField.WHEELS + BaseIngestHelper.FIELD_TYPE, NumberType.class.getName());
        this.hConf.set(this.dataType + EventRecordReader.Properties.EVENT_DATE_FIELD_NAME, CarField.START_DATE.name());
        this.hConf.set(this.dataType + EventRecordReader.Properties.EVENT_DATE_FIELD_FORMAT, DATE_FIELD_FORMAT);
        
        this.hConf.set(this.dataType + ".data.category.id.field", CarField.EVENT_ID.name());
        
        // fields
        this.hConf.set(this.dataType + CSVHelper.DATA_HEADER, String.join(",", CarField.headers()));
        
        log.debug(this.toString());
    }
    
    @Override
    public String toString() {
        return this.getClass().getSimpleName() + "{" + super.toString() + "}";
    }
}

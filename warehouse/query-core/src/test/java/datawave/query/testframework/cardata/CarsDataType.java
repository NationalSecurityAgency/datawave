package datawave.query.testframework.cardata;

import datawave.data.normalizer.Normalizer;
import datawave.data.type.NumberType;
import datawave.ingest.data.config.CSVHelper;
import datawave.ingest.data.config.ingest.BaseIngestHelper;
import datawave.ingest.input.reader.EventRecordReader;
import datawave.query.testframework.*;
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
     * Defines the valid dates that are used for shard ids. All test data should specify one of the shard id dates.
     */
    public enum CarShardId {
        // list of shards for testing
        DATE_2017_0404("20170404"),
        DATE_2017_0505("20170505"),
        DATE_2017_0606("20170606"),
        DATE_2017_0707("20170707"),
        DATE_2017_0808("20170808"),
        DATE_2017_0909("20170909"),
        DATE_2017_1010("20171010"),
        DATE_2017_1111("20171111");
        
        public static Set<String> getShardRange(final Date start, final Date end) {
            final Set<String> shards = new HashSet<>();
            for (final CarShardId id : CarShardId.values()) {
                if (0 >= start.compareTo(id.date) && 0 <= end.compareTo(id.date)) {
                    shards.add(id.dateStr);
                }
            }
            
            return shards;
        }
        
        static final List<Date> sortedDate = new ArrayList<>();
        
        public static Date[] getStartEndDates(final boolean random) {
            // use double check locking
            if (sortedDate.isEmpty()) {
                synchronized (sortedDate) {
                    if (sortedDate.isEmpty()) {
                        final List<Date> dates = new ArrayList<>();
                        for (final CarShardId id : CarShardId.values()) {
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
        
        CarShardId(final String str) {
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
        
        static Collection<String> carShads() {
            return Stream.of(CarShardId.values()).map(e -> e.getShardId()).collect(Collectors.toList());
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
        
        private static final Map<String,BaseRawData.RawMetaData> metadataMapping = new HashMap<>();
        
        private BaseRawData.RawMetaData metadata;
        
        CarField(final Normalizer<?> normalizer) {
            this(normalizer, false);
        }
        
        CarField(final Normalizer<?> normalizer, final boolean isMulti) {
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
    public Collection<String> getShardIds() {
        return CarShardId.carShads();
    }
    
    @Override
    public String toString() {
        return "CarsDataType{" + super.toString() + "}";
    }
}

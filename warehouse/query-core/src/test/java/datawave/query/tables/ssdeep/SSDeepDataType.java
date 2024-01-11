package datawave.query.tables.ssdeep;

import datawave.data.normalizer.Normalizer;
import datawave.data.type.NoOpType;
import datawave.data.type.NumberType;
import datawave.ingest.csv.config.helper.ExtendedCSVHelper;
import datawave.ingest.data.TypeRegistry;
import datawave.ingest.data.config.CSVHelper;
import datawave.ingest.data.config.ingest.BaseIngestHelper;
import datawave.ingest.input.reader.EventRecordReader;
import datawave.ingest.mapreduce.handler.shard.AbstractColumnBasedHandler;
import datawave.ingest.mapreduce.handler.ssdeep.SSDeepIndexHandler;
import datawave.marking.MarkingFunctions;
import datawave.query.testframework.AbstractDataTypeConfig;
import datawave.query.testframework.CitiesDataType;
import datawave.query.testframework.FieldConfig;
import datawave.query.testframework.RawDataManager;
import datawave.query.testframework.RawMetaData;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Contains all the relevant data needed to configure the ssdeep data type.
 */
public class SSDeepDataType extends AbstractDataTypeConfig {

    private static final Logger log = Logger.getLogger(SSDeepDataType.class);

    /**
     * Contains predefined names for the ssdeep datatype. Each enumeration will contain the path of the data ingest file. Currently, there is only one
     */
    public enum SSDeepEntry {
        // default provided cities with datatype name
        ssdeep("input/ssdeep-data.csv", "ssdeep");
  
        private final String ingestFile;
        private final String datatype;

        SSDeepEntry(final String file, final String name) {
            this.ingestFile = file;
            this.datatype = name;
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
            return this.datatype;
        }
    }

    /**
     * Defines the data fields for cities datatype.
     */
    @SuppressWarnings("SpellCheckingInspection")
    public enum SSDeepField {
        // order is important, should match the order in the csv files
        PROCESSING_DATE(Normalizer.LC_NO_DIACRITICS_NORMALIZER),
        EVENT_ID(Normalizer.LC_NO_DIACRITICS_NORMALIZER),
        LANGUAGE(Normalizer.LC_NO_DIACRITICS_NORMALIZER),
        ORIGINAL_SIZE(Normalizer.LC_NO_DIACRITICS_NORMALIZER),
        PROCESSED_SIZE(Normalizer.LC_NO_DIACRITICS_NORMALIZER),
        FILE_TYPE(Normalizer.LC_NO_DIACRITICS_NORMALIZER),
        MD5(Normalizer.LC_NO_DIACRITICS_NORMALIZER),
        SHA1(Normalizer.LC_NO_DIACRITICS_NORMALIZER),
        SHA256(Normalizer.LC_NO_DIACRITICS_NORMALIZER),
        EVENT_DATE(Normalizer.LC_NO_DIACRITICS_NORMALIZER),
        SECURITY_MARKING(Normalizer.LC_NO_DIACRITICS_NORMALIZER),
        //The folowing fields are 'extra' fields indicated with the K=V structure in the CSV.
        FILE_DATE(Normalizer.LC_NO_DIACRITICS_NORMALIZER, true),
        FILE_NAME(Normalizer.LC_NO_DIACRITICS_NORMALIZER, true),
        CHECKSUM_SSDEEP(Normalizer.NOOP_NORMALIZER, true),
        IMAGEHEIGHT(Normalizer.NUMBER_NORMALIZER, true),
        IMAGEWIDTH(Normalizer.NUMBER_NORMALIZER, true),
        PARENT_FILETYPE(Normalizer.LC_NO_DIACRITICS_NORMALIZER, true),
        ACCESS_CONTROLS(Normalizer.LC_NO_DIACRITICS_NORMALIZER, true);

        private static final List<String> HEADERS;

        static {
            HEADERS = Stream.of(SSDeepField.values()).filter(Predicate.not(SSDeepField::isExtraField)).map(Enum::name).collect(Collectors.toList());
        }

        public static List<String> headers() {
            return HEADERS;
        }

        private static final Map<String, RawMetaData> fieldMetadata;

        static {
            fieldMetadata = new HashMap<>();
            for (SSDeepField field : SSDeepField.values()) {
                fieldMetadata.put(field.name().toLowerCase(), field.metadata);
            }
        }

        /**
         * Retrieves the enumeration that matches the specified field.
         *
         * @param field
         *            string representation of the field.
         * @return enumeration value
         * @throws AssertionError
         *             field does not match any of the enumeration values
         */
        public static SSDeepField getField(final String field) {
            for (final SSDeepField f : SSDeepField.values()) {
                if (f.name().equalsIgnoreCase(field)) {
                    return f;
                }
            }

            throw new AssertionError("invalid SSDeep field(" + field + ")");
        }

        /**
         * Returns mapping of ip address fields to the metadata for the field.
         *
         * @return populate map
         */
        public static Map<String,RawMetaData> getFieldsMetadata() {
            return fieldMetadata;
        }

        private final RawMetaData metadata;

        /** A flag to set if we expect the field to be not returned as part of the headers. These fields are
         * represented in the CSC as fieldName=fieldValue pairs.*/
        private final boolean extraField;

        SSDeepField(final Normalizer<?> normalizer) {
            this(normalizer, false);
        }

        SSDeepField(final Normalizer<?> normalizer, final boolean extraField) {
            this.extraField = extraField;
            // we don't use multivalued fields for this datatype, so don't bother setting it.
            this.metadata = new RawMetaData(this.name(), normalizer, false);
        }

        /**
         * Returns the metadata for this field.
         *
         * @return metadata
         */
        public RawMetaData getMetadata() {
            return metadata;
        }

        /**
         * Returns whether the field is an 'extra' field and should not be included in the headers.
         *
         * @return metadata
         */
        public boolean isExtraField() { return extraField; }
    }

    // ==================================
    // data manager info
    private static final RawDataManager ssdeepManager = new SSDeepDataManager();

    public static RawDataManager getManager() {
        return ssdeepManager;
    }

    /**
     * Creates a ssdeep datatype entry with all the key/value configuration settings.
     *
     * @param ssdeep
     *            entry for ingest containing datatype and ingest file
     * @param config
     *            hadoop field configuration
     * @throws IOException
     *             unable to load ingest file
     * @throws URISyntaxException
     *             unable to resolve ingest file
     */
    public SSDeepDataType(final SSDeepEntry ssdeep, final FieldConfig config) throws IOException, URISyntaxException {
        this(ssdeep.getDataType(), ssdeep.getIngestFile(), config);
    }

    /**
     * Constructor for ssdeep ingest files that are not defined in the class {@link SSDeepEntry}.
     *
     * @param ssdeep
     *            name of the ssdeep datatype
     * @param ingestFile
     *            ingest file path
     * @param config
     *            hadoop field configuration
     * @throws IOException
     *             error loading test data
     * @throws URISyntaxException
     *             invalid test data file
     */
    public SSDeepDataType(final String ssdeep, final String ingestFile, final FieldConfig config) throws IOException, URISyntaxException {
        super(ssdeep, ingestFile, config, ssdeepManager);
        // NOTE: see super for default settings

        // set datatype settings
        this.hConf.set(this.dataType + "." + SSDeepField.CHECKSUM_SSDEEP.name() + BaseIngestHelper.FIELD_TYPE, NoOpType.class.getName());
        this.hConf.set(this.dataType + EventRecordReader.Properties.EVENT_DATE_FIELD_NAME, SSDeepField.PROCESSING_DATE.name());
        this.hConf.set(this.dataType + EventRecordReader.Properties.EVENT_DATE_FIELD_FORMAT, SSDEEP_DATE_FIELD_FORMAT);
        
        this.hConf.set(this.dataType + ExtendedCSVHelper.Properties.EVENT_ID_FIELD_NAME, SSDeepField.EVENT_ID.name());
        
        // fields
        this.hConf.set(this.dataType + CSVHelper.DATA_HEADER, String.join(",", SSDeepField.headers()));
        this.hConf.set(this.dataType + CSVHelper.PROCESS_EXTRA_FIELDS, "true");

        // ssdeep index handler
        this.hConf.set(this.dataType + TypeRegistry.HANDLER_CLASSES, String.join(",", AbstractColumnBasedHandler.class.getName(), SSDeepIndexHandler.class.getName()));
        this.hConf.set(this.dataType + SSDeepIndexHandler.SSDEEP_FIELD_SET, "CHECKSUM_SSDEEP");
        this.hConf.set(SSDeepIndexHandler.SSDEEP_INDEX_TABLE_NAME, SSDeepQueryTestTableHelper.SSDEEP_INDEX_TABLE_NAME);

        log.debug(this.toString());
    }

    private static final String SSDEEP_DATE_FIELD_FORMAT = "yyyy-MM-dd hh:mm:ss";

    private static final String[] AUTH_VALUES = new String[] {"public"};
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
        return ""; //TODO: while the ssdeep data has security markings, we don't use them in the tests yet.
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

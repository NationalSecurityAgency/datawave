package datawave.query.testframework;

import datawave.data.normalizer.Normalizer;
import datawave.ingest.csv.config.helper.ExtendedCSVHelper;
import datawave.ingest.data.TypeRegistry;
import datawave.ingest.data.config.CSVHelper;
import datawave.ingest.input.reader.EventRecordReader;
import datawave.ingest.json.mr.handler.ContentJsonColumnBasedHandler;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Configures a datatype for grouping data.
 */
public class BooksDataType extends AbstractDataTypeConfig {
    
    private static final Logger log = Logger.getLogger(BooksDataType.class);
    
    /**
     * Predefined instances for the books datatype.
     */
    public enum BooksEntry {
        // default provided books datatypes - use the name() as the datatype
        tech("input/java-books.csv");
        
        private final String ingestFile;
        
        BooksEntry(final String path) {
            this.ingestFile = path;
        }
        
        /**
         * Returns the datatype for the event.
         *
         * @return datatype for instance
         */
        public String getDataType() {
            return this.name();
        }
        
        public String getIngestFile() {
            return this.ingestFile;
        }
    }
    
    /**
     * Defines the fields for the books datatype.
     */
    public enum BooksField {
        // field names with normalizer
        BOOKS_DATE(Normalizer.LC_NO_DIACRITICS_NORMALIZER),
        TITLE(Normalizer.LC_NO_DIACRITICS_NORMALIZER),
        AUTHOR(Normalizer.LC_NO_DIACRITICS_NORMALIZER, true),
        NUM_PAGES(Normalizer.NUMBER_NORMALIZER),
        SUB_TITLE(Normalizer.LC_NO_DIACRITICS_NORMALIZER),
        DATE_PUBLISHED(Normalizer.LC_NO_DIACRITICS_NORMALIZER),
        LANGUAGE(Normalizer.LC_NO_DIACRITICS_NORMALIZER, true),
        ISBN_13(Normalizer.LC_NO_DIACRITICS_NORMALIZER),
        ISBN_10(Normalizer.LC_NO_DIACRITICS_NORMALIZER);
        
        private static final List<String> headers;
        static {
            headers = Stream.of(BooksField.values()).map(e -> e.name()).collect(Collectors.toList());
        }
        
        public static List<String> getHeaders() {
            return headers;
        }
        
        private static final Map<String,RawMetaData> fieldMetadata;
        static {
            fieldMetadata = new HashMap<>();
            for (BooksField field : BooksField.values()) {
                fieldMetadata.put(field.name().toLowerCase(), field.metadata);
            }
        }
        
        public static Map<String,RawMetaData> getFieldsMetadata() {
            return fieldMetadata;
        }
        
        private final RawMetaData metadata;
        
        BooksField(Normalizer<?> norm) {
            this(norm, false);
        }
        
        BooksField(Normalizer<?> norm, boolean multi) {
            this.metadata = new RawMetaData(this.name(), norm, multi);
        }
        
        public RawMetaData getMetadata() {
            return this.metadata;
        }
    }
    
    private final FieldConfig fieldConfig;
    private final ConfigData configData;
    
    public BooksDataType(final String datatype, final String file, final FieldConfig fieldData, final ConfigData cfgData) throws IOException,
                    URISyntaxException {
        super(datatype, file, FileType.GROUPING, fieldData, null);
        this.fieldConfig = fieldData;
        this.configData = cfgData;
        
        // NOTE: see super for default settings
        // set datatype settings
        this.hConf.set(this.dataType + EventRecordReader.Properties.EVENT_DATE_FIELD_NAME, cfgData.getDateField());
        this.hConf.set(this.dataType + EventRecordReader.Properties.EVENT_DATE_FIELD_FORMAT, DATE_FIELD_FORMAT);
        
        this.hConf.set(this.dataType + ExtendedCSVHelper.Properties.EVENT_ID_FIELD_NAME, cfgData.getEventId());
        
        // fields
        this.hConf.set(this.dataType + CSVHelper.DATA_HEADER, String.join(",", cfgData.headers()));
        
        this.hConf.set(this.dataType + TypeRegistry.HANDLER_CLASSES, ContentJsonColumnBasedHandler.class.getName());
        
        // needed for hierarchical context
        this.hConf.set(this.dataType + CSVHelper.PROCESS_EXTRA_FIELDS, "true");
        
        // load raw test data into the data manager
        Set<String> anyFieldIndexes = new HashSet<>(fieldConfig.getIndexFields());
        anyFieldIndexes.addAll(fieldConfig.getReverseIndexFields());
        
        log.debug(this.toString());
    }
    
    public ConfigData getData() {
        return this.configData;
    }
    
    @Override
    public String toString() {
        return this.getClass().getSimpleName() + "{" + super.toString() + "}";
    }
    
}

package datawave.query.testframework;

import datawave.data.normalizer.Normalizer;
import datawave.ingest.csv.config.helper.ExtendedCSVHelper;
import datawave.ingest.data.TypeRegistry;
import datawave.ingest.data.config.CSVHelper;
import datawave.ingest.input.reader.EventRecordReader;
import datawave.ingest.json.mr.handler.ContentJsonColumnBasedHandler;
import org.apache.accumulo.core.client.Connector;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Collection;
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
     * List of books that are used for testing.
     */
    public enum BooksEntry {
        // default provided books datatypes
        tech("input/java-books.csv");
        
        private final String ingestFile;
        
        BooksEntry(final String path) {
            this.ingestFile = path;
        }
        
        /**
         * Returns the datatype for the entry.
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
    
    private final FieldConfig fieldConfig;
    private final ConfigData configData;
    
    public BooksDataType(final String datatype, final String file, final FieldConfig fieldData, final ConfigData cfgData) throws IOException,
                    URISyntaxException {
        super(datatype, file, FileLoaderFactory.FileType.GROUPING, fieldData, null);
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
    public Collection<String> getShardIds() {
        return BaseShardIdRange.getShardDates();
    }
    
    @Override
    public String toString() {
        return "BooksDataType{" + super.toString() + "}";
    }
    
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
        
        private static final Map<String,BaseRawData.RawMetaData> metadataMapping = new HashMap<>();
        static {
            for (BooksField field : BooksField.values()) {
                BaseRawData.RawMetaData data = new BaseRawData.RawMetaData(field.name(), field.normalizer, field.multiValue);
                metadataMapping.put(field.name().toLowerCase(), data);
            }
        }
        
        public static Map<String,BaseRawData.RawMetaData> getMetadata() {
            return metadataMapping;
        }
        
        private final Normalizer normalizer;
        private final boolean multiValue;
        
        BooksField(Normalizer<?> norm) {
            this(norm, false);
        }
        
        BooksField(Normalizer<?> norm, boolean isMulti) {
            this.normalizer = norm;
            this.multiValue = isMulti;
        }
    }
}

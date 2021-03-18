package datawave.query.testframework;

import datawave.data.normalizer.Normalizer;
import datawave.ingest.csv.config.helper.ExtendedCSVHelper;
import datawave.ingest.data.TypeRegistry;
import datawave.ingest.data.config.CSVHelper;
import datawave.ingest.input.reader.EventRecordReader;
import datawave.ingest.json.config.helper.JsonDataTypeHelper;
import datawave.ingest.json.mr.handler.ContentJsonColumnBasedHandler;
import datawave.ingest.json.util.JsonObjectFlattener.FlattenMode;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Handles JSON flatten datatypes. Fields for this datatype are defined based upon the {@link FlattenMode} and are defined with the appropriate test class.
 */
public class FlattenDataType extends AbstractDataTypeConfig {
    
    private static final Logger log = Logger.getLogger(FlattenDataType.class);
    
    public enum FlattenEntry {
        // predefined grouping data
        cityFlatten("input/city-flatten.json", "flatten");
        
        private final String ingestFile;
        private final String datatype;
        
        FlattenEntry(final String file, final String name) {
            this.ingestFile = file;
            this.datatype = name;
        }
        
        private String getIngestFile() {
            return this.ingestFile;
        }
        
        private String getDatatype() {
            return this.datatype;
        }
    }
    
    /**
     * This is the base normalizer for all flatten test classes. These fields must be included in the fields for each different flatten query test class.
     */
    public enum FlattenBaseFields {
        // base fields
        STARTDATE(Normalizer.LC_NO_DIACRITICS_NORMALIZER),
        EVENTID(Normalizer.LC_NO_DIACRITICS_NORMALIZER);
        
        private final Normalizer<?> normalizer;
        
        FlattenBaseFields(final Normalizer<?> norm) {
            this.normalizer = norm;
        }
        
        public Normalizer<?> getNormalizer() {
            return this.normalizer;
        }
    }
    
    // ==================================
    // data manager for flatten is based upon the flatten mode - metadata may be different for each mode
    private static volatile Map<FlattenMode,RawDataManager> manager = new EnumMap<>(FlattenMode.class);
    
    public static RawDataManager getManager(final FlattenData data) {
        FlattenMode mode = data.getMode();
        if (!manager.containsKey(mode)) {
            synchronized (manager) {
                if (!manager.containsKey(mode)) {
                    manager.put(mode, new FlattenDataManager(data));
                }
            }
        }
        return manager.get(mode);
    }
    
    private static final Map<String,FlattenDataType> flattenTypes = new HashMap<>();
    
    static FlattenDataType getFlattenDataType(final String dataType) {
        return flattenTypes.get(dataType);
    }
    
    /**
     * Contains the flatten data for a specific flatten test class.
     */
    private final FlattenData flatData;
    
    /**
     * Creates a groups datatype entry with all of the key/value configuration settings.
     *
     * @param group
     *            entry for ingest containing datatype and ingest file
     * @param config
     *            hadoop field configuration
     * @param data
     *            flatten data for test instance
     * @throws IOException
     *             unable to load ingest file
     * @throws URISyntaxException
     *             unable to resolve ingest file
     */
    public FlattenDataType(final FlattenEntry group, final FieldConfig config, final FlattenData data) throws IOException, URISyntaxException {
        this(group.getDatatype(), group.getIngestFile(), config, data);
    }
    
    /**
     * Constructor for groups ingest files that are not defined in the class {@link FlattenEntry}.
     *
     * @param datatype
     *            name of the datatype
     * @param ingestFile
     *            ingest file path
     * @param fieldConfig
     *            hadoop field configuration
     * @param flattenData
     *            flatten data for test instance
     * @throws IOException
     *             error loading ingest data
     * @throws URISyntaxException
     *             ingest file name error
     */
    public FlattenDataType(final String datatype, final String ingestFile, final FieldConfig fieldConfig, final FlattenData flattenData) throws IOException,
                    URISyntaxException {
        super(datatype, ingestFile, FileType.JSON, fieldConfig, manager.get(flattenData.getMode()));
        
        this.flatData = flattenData;
        
        // NOTE: see super for default settings
        // set datatype settings
        this.hConf.set(this.dataType + EventRecordReader.Properties.EVENT_DATE_FIELD_NAME, flattenData.getDateField());
        this.hConf.set(this.dataType + EventRecordReader.Properties.EVENT_DATE_FIELD_FORMAT, DATE_FIELD_FORMAT);
        
        this.hConf.set(this.dataType + ExtendedCSVHelper.Properties.EVENT_ID_FIELD_NAME, flattenData.getEventId());
        
        // fields
        this.hConf.set(this.dataType + CSVHelper.DATA_HEADER, String.join(",", flattenData.headers()));
        
        this.hConf.set(this.dataType + TypeRegistry.HANDLER_CLASSES, ContentJsonColumnBasedHandler.class.getName());
        this.hConf.set(this.dataType + JsonDataTypeHelper.Properties.FLATTENER_MODE, flattenData.getMode().name());
        
        // needed for hierarchical context
        this.hConf.set(this.dataType + CSVHelper.PROCESS_EXTRA_FIELDS, "true");
        
        flattenTypes.put(this.dataType, this);
        
        // load raw test data into the data manager
        Set<String> anyFieldIndexes = new HashSet<>(fieldConfig.getIndexFields());
        anyFieldIndexes.addAll(fieldConfig.getReverseIndexFields());
        RawDataManager curMgr = manager.get(flattenData.getMode());
        curMgr.addTestData(this.ingestPath, this.dataType, anyFieldIndexes);
        
        log.debug(this.toString());
    }
    
    public FlattenData getFlattenData() {
        return this.flatData;
    }
    
    @Override
    public String toString() {
        return this.getClass().getSimpleName() + "{" + super.toString() + "}";
    }
}

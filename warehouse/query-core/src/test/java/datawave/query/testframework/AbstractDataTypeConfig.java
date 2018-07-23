package datawave.query.testframework;

import datawave.data.type.LcNoDiacriticsType;
import datawave.data.type.NoOpType;
import datawave.ingest.csv.config.helper.ExtendedCSVIngestHelper;
import datawave.ingest.csv.mr.input.CSVRecordReader;
import datawave.ingest.data.TypeRegistry;
import datawave.ingest.data.config.CSVHelper;
import datawave.ingest.data.config.DataTypeHelper;
import datawave.ingest.data.config.MarkingsHelper;
import datawave.ingest.data.config.ingest.BaseIngestHelper;
import datawave.ingest.data.config.ingest.CompositeIngest;
import datawave.ingest.data.config.ingest.VirtualIngest;
import datawave.ingest.mapreduce.handler.shard.AbstractColumnBasedHandler;
import datawave.ingest.mapreduce.handler.shard.ShardedDataTypeHandler;
import datawave.policy.IngestPolicyEnforcer;
import datawave.query.Constants;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.junit.Assert;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Abstract base class that contains the configuration settings for test data types.
 */
public abstract class AbstractDataTypeConfig implements IDataTypeHadoopConfig {
    
    private static final Logger log = Logger.getLogger(AbstractDataTypeConfig.class);
    
    // ===============================================
    // common constants for all data types
    protected static final String DATE_FIELD_FORMAT = "yyyyMMdd";
    public static final SimpleDateFormat YMD_DateFormat = new SimpleDateFormat(DATE_FIELD_FORMAT);
    private static final String AUTH_VALUES = "public";
    private static final Authorizations TEST_AUTHS = new Authorizations(AUTH_VALUES);
    
    /**
     * Retrieves an {@link Authorizations} object to use for query.
     *
     * @return valid auths object
     */
    public static Authorizations getTestAuths() {
        return TEST_AUTHS;
    }
    
    private static List<String> mappingToNames(final Collection<Set<String>> mapping) {
        final List<String> names = new ArrayList<>();
        for (final Set<String> composite : mapping) {
            names.add(String.join("_", composite));
        }
        
        return names;
    }
    
    private static List<String> mappingToFields(final Collection<Set<String>> mapping) {
        final List<String> fields = new ArrayList<>();
        for (final Set<String> virtual : mapping) {
            fields.add(String.join(".", virtual));
        }
        return fields;
    }
    
    // ===============================================
    // inherited instance members
    protected final String dataType;
    protected final URI ingestPath;
    /**
     * Includes on the fields that are indexed (none of the composite fields).
     */
    private final IFieldConfig fieldConfig;
    protected final Configuration hConf = new Configuration();
    
    /**
     * @param dt
     *            datatype name
     * @param ingestFile
     *            ingest file name
     * @param config
     *            field config for accumulo
     * @param manager
     *            data manager for data
     * @throws IOException
     *             unable to load ingest file
     * @throws URISyntaxException
     *             ingest file uri conversion error
     */
    protected AbstractDataTypeConfig(final String dt, final String ingestFile, final IFieldConfig config, final IRawDataManager manager) throws IOException,
                    URISyntaxException {
        log.info("---------  loading datatype (" + dt + ") ingest file(" + ingestFile + ") ---------");
        
        // IRawDataManager manager = mgr;
        URL url = this.getClass().getClassLoader().getResource(ingestFile);
        Assert.assertNotNull("unable to resolve ingest file(" + ingestFile + ")", url);
        this.ingestPath = url.toURI();
        this.dataType = dt;
        this.fieldConfig = config;
        
        // load raw data into POJO
        Set<String> anyFieldIndexes = new HashSet<>(this.fieldConfig.getIndexFields());
        anyFieldIndexes.addAll(this.fieldConfig.getReverseIndexFields());
        manager.addTestData(this.ingestPath, this.dataType, anyFieldIndexes);
        
        // default Hadoop settings - override if needed
        this.hConf.set(DataTypeHelper.Properties.DATA_NAME, this.dataType);
        
        this.hConf.set(this.dataType + TypeRegistry.INGEST_HELPER, ExtendedCSVIngestHelper.class.getName());
        this.hConf.set(this.dataType + TypeRegistry.RAW_READER, CSVRecordReader.class.getName());
        this.hConf.set(this.dataType + TypeRegistry.HANDLER_CLASSES, AbstractColumnBasedHandler.class.getName());
        this.hConf.set(this.dataType + BaseIngestHelper.DEFAULT_TYPE, LcNoDiacriticsType.class.getName());
        this.hConf.set(this.dataType + CSVHelper.DATA_SEP, ",");
        this.hConf.set(this.dataType + CSVHelper.MULTI_VALUED_SEPARATOR, IRawDataManager.MULTIVALUE_SEP);
        
        this.hConf.set(this.dataType + ".ingest.policy.enforcer.class", IngestPolicyEnforcer.NoOpIngestPolicyEnforcer.class.getName());
        
        this.hConf.set(this.dataType + ".data.auth.id.mode", "NEVER");
        this.hConf.set(ShardedDataTypeHandler.METADATA_TERM_FREQUENCY, "true");
        
        this.hConf.set(this.dataType + MarkingsHelper.DEFAULT_MARKING, AUTH_VALUES);
        
        // composite/virtual field separator
        this.hConf.set(this.dataType + CompositeIngest.COMPOSITE_FIELD_VALUE_SEPARATOR, Constants.MAX_UNICODE_STRING);
        this.hConf.set(this.dataType + VirtualIngest.VIRTUAL_FIELD_VALUE_SEPARATOR, "|");
        
        // index fields also include composite fields
        Set<String> indexEntries = new HashSet<>(this.fieldConfig.getIndexFields());
        List<String> compositeNames = mappingToNames(this.fieldConfig.getCompositeFields());
        indexEntries.addAll(compositeNames);
        this.hConf.set(this.dataType + BaseIngestHelper.INDEX_FIELDS, String.join(",", indexEntries));
        this.hConf.set(this.dataType + BaseIngestHelper.INDEX_ONLY_FIELDS, String.join(",", this.fieldConfig.getIndexOnlyFields()));
        this.hConf.set(this.dataType + BaseIngestHelper.REVERSE_INDEX_FIELDS, String.join(",", this.fieldConfig.getReverseIndexFields()));
        this.hConf.set(this.dataType + CompositeIngest.COMPOSITE_FIELD_NAMES, String.join(",", mappingToNames(this.fieldConfig.getCompositeFields())));
        this.hConf.set(this.dataType + CompositeIngest.COMPOSITE_FIELD_MEMBERS, String.join(",", mappingToFields(this.fieldConfig.getCompositeFields())));
        
        // type for composite fields
        for (final String composite : compositeNames) {
            this.hConf.set(this.dataType + "." + composite, NoOpType.class.getName());
        }
        
        // virtual fields
        this.hConf.set(this.dataType + VirtualIngest.VIRTUAL_FIELD_NAMES, String.join(",", mappingToNames(this.fieldConfig.getVirtualFields())));
        this.hConf.set(this.dataType + VirtualIngest.VIRTUAL_FIELD_MEMBERS, String.join(",", mappingToFields(this.fieldConfig.getVirtualFields())));
        
        // multivalue fields
        this.hConf.set(this.dataType + CSVHelper.MULTI_VALUED_FIELDS, String.join(",", this.fieldConfig.getMultiValueFields()));
        
    }
    
    @Override
    public Configuration getHadoopConfiguration() {
        return this.hConf;
    }
    
    @Override
    public String dataType() {
        return this.dataType;
    }
    
    @Override
    public URI getIngestFile() {
        return this.ingestPath;
    }
    
    @Override
    public String toString() {
        return "AbstractDataTypeConfig{" + "dataType='" + dataType + '\'' + ", ingestPath=" + ingestPath + ", fieldConfig=" + fieldConfig + '}';
    }
}

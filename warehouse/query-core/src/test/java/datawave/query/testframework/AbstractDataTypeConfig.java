package datawave.query.testframework;

import datawave.data.type.LcNoDiacriticsType;
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

/**
 * Abstract base class that contains the configuration settings for test data types.
 */
public abstract class AbstractDataTypeConfig implements IDataTypeHadoopConfig {
    
    private static final Logger log = Logger.getLogger(AbstractDataTypeConfig.class);
    
    // ===============================================
    // common constants for all data types
    protected static final String DATE_FIELD_FORMAT = "yyyyMMdd";
    public static final SimpleDateFormat YMD_DateFormat = new SimpleDateFormat(DATE_FIELD_FORMAT);
    protected static final String AUTH_VALUES = "public";
    private static final Authorizations TEST_AUTHS = new Authorizations(AUTH_VALUES);
    
    // ===============================================
    // inherited instance members
    protected final String dataType;
    protected final URI ingestPath;
    protected final Configuration hConf = new Configuration();
    private final IRawDataManager manager;
    
    /**
     * Retrieves an {@link Authorizations} object to use for query.
     *
     * @return valid auths object
     */
    public static Authorizations getTestAuths() {
        return TEST_AUTHS;
    }
    
    /**
     * @param dt
     *            datatype name
     * @param ingestFile
     *            ingest file name
     * @throws IOException
     *             unable to load ingest file
     * @throws URISyntaxException
     *             ingest file uri conversion error
     */
    protected AbstractDataTypeConfig(String dt, String ingestFile, IRawDataManager mgr) throws IOException, URISyntaxException {
        log.info("---------  loading datatype (" + dt + ") ingest file(" + ingestFile + ") ---------");
        
        this.manager = mgr;
        URL url = this.getClass().getClassLoader().getResource(ingestFile);
        Assert.assertNotNull("unable to resolve ingest file(" + ingestFile + ")", url);
        this.ingestPath = url.toURI();
        this.dataType = dt;
        
        // load raw data into POJO
        this.manager.addTestData(this.ingestPath);
        
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
        this.hConf.set(this.dataType + ShardedDataTypeHandler.METADATA_TERM_FREQUENCY, "true");
        
        this.hConf.set(this.dataType + MarkingsHelper.DEFAULT_MARKING, AUTH_VALUES);
        
        // composite/virtual field separator
        this.hConf.set(this.dataType + CompositeIngest.COMPOSITE_FIELD_VALUE_SEPARATOR, Constants.MAX_UNICODE_STRING);
        this.hConf.set(this.dataType + VirtualIngest.VIRTUAL_FIELD_VALUE_SEPARATOR, "|");
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
}

package nsa.datawave.ingest.mapreduce.handler.summary;

import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.Multimap;

import nsa.datawave.ingest.data.RawRecordContainer;
import nsa.datawave.ingest.data.Type;
import nsa.datawave.ingest.data.TypeRegistry;
import nsa.datawave.ingest.data.config.NormalizedContentInterface;
import nsa.datawave.ingest.data.config.ingest.IngestHelperInterface;
import nsa.datawave.ingest.mapreduce.handler.DataTypeHandler;
import nsa.datawave.ingest.mapreduce.job.BulkIngestKey;
import nsa.datawave.ingest.metadata.RawRecordMetadata;
import nsa.datawave.webservice.common.logging.ThreadConfigurableLogger;

import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;

public abstract class SummaryDataTypeHandler<KEYIN> implements DataTypeHandler<KEYIN> {
    private static final Logger log = ThreadConfigurableLogger.getLogger(SummaryDataTypeHandler.class);
    
    private Map<String,IngestHelperInterface> mIngestHelpersCache = null;
    private Configuration mConf = null;
    
    protected abstract Multimap<BulkIngestKey,Value> createEntries(RawRecordContainer record, Multimap<String,NormalizedContentInterface> fields,
                    ColumnVisibility vis, long timestampe, IngestHelperInterface iHelper);
    
    @Override
    public void setup(TaskAttemptContext context) {
        Configuration conf = context.getConfiguration();
        mIngestHelpersCache = new HashMap<>(10);
        TypeRegistry.getInstance(conf);
        mConf = conf;
    }
    
    @Override
    public Multimap<BulkIngestKey,Value> processBulk(KEYIN key, RawRecordContainer record, Multimap<String,NormalizedContentInterface> fields,
                    StatusReporter reporter) {
        IngestHelperInterface iHelper = this.getHelper(record.getDataType());
        Multimap<BulkIngestKey,Value> values = createEntries(record, fields, getVisibility(), System.currentTimeMillis(), iHelper);
        return values;
    }
    
    // Since this is a summary use the configured default classification and access controls for column visibility
    private ColumnVisibility getVisibility() {
        ColumnVisibility vis;
        vis = new ColumnVisibility();
        return vis;
    }
    
    @Override
    public IngestHelperInterface getHelper(Type datatype) {
        final String typeName = datatype.typeName();
        if (mIngestHelpersCache.containsKey(typeName)) {
            return mIngestHelpersCache.get(typeName);
        } else {
            IngestHelperInterface ingestHelper = null;
            try {
                ingestHelper = TypeRegistry.getType(typeName).getHelperClass().newInstance();
                ingestHelper.setup(mConf);
                mIngestHelpersCache.put(typeName, ingestHelper);
            } catch (Exception e) {
                log.error("Unable to get an instance of ingest helper class for datatype: " + typeName, e);
            }
            return ingestHelper;
        }
        
    }
    
    @Override
    public void close(TaskAttemptContext context) {
        // do nothing
    }
    
    @Override
    public RawRecordMetadata getMetadata() {
        return null; // do nothing
    }
}

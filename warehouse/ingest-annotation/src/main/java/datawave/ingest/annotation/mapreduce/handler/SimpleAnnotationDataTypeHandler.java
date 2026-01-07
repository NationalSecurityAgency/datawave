package datawave.ingest.annotation.mapreduce.handler;

import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.Type;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.data.config.ingest.IngestHelperInterface;
import datawave.ingest.mapreduce.handler.DataTypeHandler;
import datawave.ingest.mapreduce.job.BulkIngestKey;
import datawave.ingest.metadata.RawRecordMetadata;

public class SimpleAnnotationDataTypeHandler<KEYIN> implements DataTypeHandler<KEYIN> {
    private static final Logger log = Logger.getLogger(SimpleAnnotationDataTypeHandler.class);

    protected Configuration conf = null;

    // for simplicity, I'm going to wire a custom annotation helper directly to the DataTypeHandler
    // ANNOTATION_HELPER_INTEGRATION_POINT
    private AnnotationHelper annotationHelper = null;

    @Override
    public void setup(TaskAttemptContext context) {
        this.conf = context.getConfiguration();

        // ANNOTATION_HELPER_INTEGRATION_POINT
        annotationHelper = new AnnotationHelper(conf);
    }

    @Override
    public String[] getTableNames(Configuration conf) {
        // ANNOTATION_HELPER_INTEGRATION_POINT
        if (annotationHelper == null) {
            annotationHelper = new AnnotationHelper(conf);
        }
        return annotationHelper.getAnnotationTableNames(null);
    }

    @Override
    public int[] getTableLoaderPriorities(Configuration conf) {
        // ANNOTATION_HELPER_INTEGRATION_POINT
        if (annotationHelper == null) {
            annotationHelper = new AnnotationHelper(conf);
        }
        return annotationHelper.getAnnotationTableLoaderPriorities(null);
    }

    @Override
    public Multimap<BulkIngestKey,Value> processBulk(KEYIN key, RawRecordContainer event, Multimap<String,NormalizedContentInterface> fields,
                    StatusReporter reporter) {
        Multimap<BulkIngestKey,Value> values = HashMultimap.create();

        // ANNOTATION_HELPER_INTEGRATION_POINT
        values.putAll(annotationHelper.processBulk(event.getRawData(), event, fields));

        return values;
    }

    @Override
    public IngestHelperInterface getHelper(Type datatype) {
        return datatype.getIngestHelper(conf);
    }

    @Override
    public void close(TaskAttemptContext context) {}

    @Override
    public RawRecordMetadata getMetadata() {
        return null;
    }
}

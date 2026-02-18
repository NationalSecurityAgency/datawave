package datawave.ingest.annotation.mapreduce.handler;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.log4j.Logger;

import com.google.common.collect.Multimap;

import datawave.data.hash.UID;
import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.Type;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.data.config.ingest.IngestHelperInterface;
import datawave.ingest.mapreduce.ContextWrappedStatusReporter;
import datawave.ingest.mapreduce.handler.ExtendedDataTypeHandler;
import datawave.ingest.mapreduce.handler.shard.AbstractColumnBasedHandler;
import datawave.ingest.mapreduce.job.writer.ContextWriter;

public class ShardedAnnotationDataTypeHandler<KEYIN,KEYOUT,VALUEOUT> extends AbstractColumnBasedHandler<KEYIN>
                implements ExtendedDataTypeHandler<KEYIN,KEYOUT,VALUEOUT> {
    private static final Logger log = Logger.getLogger(ShardedAnnotationDataTypeHandler.class);
    protected Configuration conf = null;

    // ANNOTATION_HELPER_INTEGRATION_POINT
    private AnnotationHelper annotationHelper = null;

    @Override
    public void setup(TaskAttemptContext context) {
        super.setup(context);
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
        return annotationHelper.getAnnotationTableNames(super.getTableNames(conf));
    }

    @Override
    public int[] getTableLoaderPriorities(Configuration conf) {
        // ANNOTATION_HELPER_INTEGRATION_POINT
        if (annotationHelper == null) {
            annotationHelper = new AnnotationHelper(conf);
        }
        return annotationHelper.getAnnotationTableLoaderPriorities(super.getTableLoaderPriorities(conf));
    }

    @Override
    public IngestHelperInterface getHelper(Type datatype) {
        return helper;
    }

    @Override
    public long process(KEYIN key, RawRecordContainer event, Multimap<String,NormalizedContentInterface> eventFields,
                    TaskInputOutputContext<KEYIN,? extends RawRecordContainer,KEYOUT,VALUEOUT> context, ContextWriter<KEYOUT,VALUEOUT> contextWriter)
                    throws IOException, InterruptedException {
        super.processBulk(key, event, eventFields, new ContextWrappedStatusReporter(getContext(context)));
        StatusReporter reporter = new ContextWrappedStatusReporter(getContext(context));

        byte[] shardId = getShardId(event);
        UID uid = event.getId();
        byte[] visibility = event.getVisibility().flatten();

        // ANNOTATION_HELPER_INTEGRATION_POINT
        if (!event.fatalError()) {
            annotationHelper.process(event, contextWriter, context, reporter, uid, visibility, shardId, event.getRawData());
        }

        return 0;
    }
}

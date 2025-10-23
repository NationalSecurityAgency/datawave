package datawave.ingest.annotation.mapreduce.handler;

import java.util.Iterator;
import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;

import datawave.annotation.data.AnnotationSerializationException;
import datawave.annotation.data.AnnotationSerializer;
import datawave.annotation.data.v1.AccumuloAnnotationSerializer;
import datawave.annotation.protobuf.v1.Annotation;
import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.Type;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.data.config.ingest.IngestHelperInterface;
import datawave.ingest.mapreduce.handler.DataTypeHandler;
import datawave.ingest.mapreduce.job.BulkIngestKey;
import datawave.ingest.metadata.RawRecordMetadata;

public class SimpleAnnotationDataTypeHandler<KEYIN> implements DataTypeHandler<KEYIN> {
    private static final Logger log = Logger.getLogger(SimpleAnnotationDataTypeHandler.class);

    // TODO: this should be configured as a part of the ingest configuration
    public static final String annotationTableName = "datawave.annotation";
    public static final Text annotationTableNameText = new Text(annotationTableName);

    protected Configuration conf = null;

    @Override
    public void setup(TaskAttemptContext context) {
        this.conf = context.getConfiguration();
    }

    @Override
    public String[] getTableNames(Configuration conf) {
        return new String[] {annotationTableName};
    }

    @Override
    public int[] getTableLoaderPriorities(Configuration conf) {
        return new int[] {10};
    }

    @Override
    public Multimap<BulkIngestKey,Value> processBulk(KEYIN key, RawRecordContainer event, Multimap<String,NormalizedContentInterface> fields,
                    StatusReporter reporter) {
        Multimap<BulkIngestKey,Value> values = HashMultimap.create();

        try {
            Annotation.Builder annotationBuilder = Annotation.newBuilder();
            JsonFormat.parser().merge(new String(event.getRawData()), annotationBuilder);
            Annotation annotation = annotationBuilder.build();
            AnnotationSerializer<Iterator<Map.Entry<Key,Value>>,Annotation> serializer = new AccumuloAnnotationSerializer();
            Iterator<Map.Entry<Key,Value>> annotationKeyIterator = serializer.serialize(annotation);

            while (annotationKeyIterator.hasNext()) {
                Map.Entry<Key,Value> entry = annotationKeyIterator.next();
                values.put(new BulkIngestKey(annotationTableNameText, entry.getKey()), entry.getValue());
            }

        } catch (InvalidProtocolBufferException | AnnotationSerializationException e) {
            log.error(e);
            // failing the record
            throw new RuntimeException(e);
        }

        return values;
    }

    @Override
    public IngestHelperInterface getHelper(Type datatype) {
        return datatype.getIngestHelper(conf);
    }

    @Override
    public void close(TaskAttemptContext context) {

    }

    @Override
    public RawRecordMetadata getMetadata() {
        return null;
    }
}

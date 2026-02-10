package datawave.ingest.annotation.mapreduce.handler;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.protobuf.Descriptors;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;

import datawave.annotation.data.transform.AnnotationTransformException;
import datawave.annotation.data.transform.DefaultTimestampTransformer;
import datawave.annotation.data.transform.DefaultVisibilityTransformer;
import datawave.annotation.data.transform.TimestampTransformer;
import datawave.annotation.data.transform.VisibilityTransformer;
import datawave.annotation.protobuf.v1.Annotation;
import datawave.data.hash.HashUID;
import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.TypeRegistry;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.data.config.NormalizedFieldAndValue;
import datawave.ingest.data.config.ingest.BaseIngestHelper;

/**
 * parses the json data and extracts a few required fields to the event
 */
public class SimpleAnnotationIngestHelper extends BaseIngestHelper {
    private static final Logger log = Logger.getLogger(SimpleAnnotationIngestHelper.class);

    private static final VisibilityTransformer DEFAULT_VISIBILITY_TRANSFORMER = new DefaultVisibilityTransformer();
    private static final TimestampTransformer DEFAULT_TIMESTAMP_TRANSFORMER = new DefaultTimestampTransformer();

    @Override
    public void setup(Configuration config) {
        super.setup(config);
    }

    @Override
    public Multimap<String,NormalizedContentInterface> getEventFields(RawRecordContainer event) {
        HashMultimap<String,NormalizedContentInterface> fields = HashMultimap.create();

        try {
            Annotation.Builder annotationBuilder = Annotation.newBuilder();
            JsonFormat.parser().merge(new String(event.getRawData()), annotationBuilder);
            Annotation annotation = annotationBuilder.build();

            event.setId(HashUID.parse(annotation.getUid()));
            event.setDataType(TypeRegistry.getType(annotation.getDataType()));
            event.setTimestamp(DEFAULT_TIMESTAMP_TRANSFORMER.fromMetadataMap(annotation.getMetadataMap()));
            event.setVisibility(DEFAULT_VISIBILITY_TRANSFORMER.fromMetadataMap(annotation.getMetadataMap()));

            for (Map.Entry<Descriptors.FieldDescriptor,Object> entry : annotation.getAllFields().entrySet()) {
                fields.put(entry.getKey().getName(), new NormalizedFieldAndValue(entry.getKey().getName(), entry.getValue().toString()));
            }
        } catch (InvalidProtocolBufferException | AnnotationTransformException e) {
            log.error(e);
            throw new RuntimeException(e);
        }

        return fields;
    }
}

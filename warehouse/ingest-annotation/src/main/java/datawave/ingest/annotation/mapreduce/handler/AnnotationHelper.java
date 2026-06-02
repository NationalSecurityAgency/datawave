package datawave.ingest.annotation.mapreduce.handler;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.FileSystemNotFoundException;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;

import javax.xml.transform.stream.StreamSource;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.log4j.Logger;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;

import datawave.annotation.data.AnnotationSerializationException;
import datawave.annotation.data.AnnotationSerializer;
import datawave.annotation.data.v1.AccumuloAnnotationSerializer;
import datawave.annotation.data.v1.AccumuloAnnotationSourceSerializer;
import datawave.annotation.protobuf.v1.Annotation;
import datawave.annotation.util.v1.AnnotationUtils;
import datawave.data.hash.UID;
import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.mapreduce.job.BulkIngestKey;
import datawave.ingest.mapreduce.job.writer.ContextWriter;
import datawave.util.time.DateHelper;
import net.sf.saxon.s9api.JsonBuilder;
import net.sf.saxon.s9api.Processor;
import net.sf.saxon.s9api.SaxonApiException;
import net.sf.saxon.s9api.XdmValue;
import net.sf.saxon.s9api.Xslt30Transformer;
import net.sf.saxon.s9api.XsltCompiler;
import net.sf.saxon.s9api.XsltExecutable;

/**
 * custom helper for ingesting annotation data. needs to be configured like SimpleAnnotationDataTypeHandler/ShardedAnnotationDataTypeHandle
 *
 */
public class AnnotationHelper {
    private static final Logger log = Logger.getLogger(AnnotationHelper.class);

    public static final String ANNOTATION_TNAME = "annotation.table.name";
    private final String annotationTableName;
    private final Text annotationTableNameText;

    public static final String ANNOTATION_SOURCE_TNAME = "annotation.source.table.name";
    private final String annotationSourceTableName;
    private final Text annotationSourceTableNameText;

    public static final String ANNOTATION_TABLE_LOAD_PRIORITY = "annotation.table.loader.priority";
    private final int annotationTableLoaderPriority;

    public static final String ANNOTATION_SOURCE_TABLE_LOAD_PRIORITY = "annotation.source.table.loader.priority";
    private final int annotationSourceTableLoaderPriority;

    public static final String ANNOTATION_RAW_TRANSFORMATION_ENABLED = "annotation.raw.transform.enable";
    private final boolean annotationRawTransformationEnabled;

    public static final String ANNOTATION_RAW_TRANSFORMATION_CONFIG = "annotation.raw.transform.config";
    private Xslt30Transformer xslt30Transformer;

    public static final String ANNOTATION_SOURCE_VISIBILITY_INHERIT_EVENT = "annotation.source.visibility.inherit.event";
    private final boolean annotationSourceVisibilityInheritEvent;

    public static final String ANNOTATION_SOURCE_VISIBILITY_DEFAULT = "annotation.source.visibility.default";
    private final String annotationSourceVisbilityDefault;

    public static final String ANNOTATION_IGNORE_UNKNOWN_FIELDS = "annotation.ignore.unknown.fields";
    private final boolean annotationIgnoreUnknownFields;

    public AnnotationHelper(Configuration conf) {
        this.annotationTableName = conf.get(ANNOTATION_TNAME, "datawave.annotation");
        this.annotationTableNameText = new Text(annotationTableName);

        this.annotationSourceTableName = conf.get(ANNOTATION_SOURCE_TNAME, "datawave.annotationSource");
        this.annotationSourceTableNameText = new Text(annotationSourceTableName);

        this.annotationTableLoaderPriority = conf.getInt(ANNOTATION_TABLE_LOAD_PRIORITY, 50);
        this.annotationSourceTableLoaderPriority = conf.getInt(ANNOTATION_SOURCE_TABLE_LOAD_PRIORITY, 60);

        this.annotationRawTransformationEnabled = conf.getBoolean(ANNOTATION_RAW_TRANSFORMATION_ENABLED, false);

        this.annotationSourceVisibilityInheritEvent = conf.getBoolean(ANNOTATION_SOURCE_VISIBILITY_INHERIT_EVENT, false);
        this.annotationSourceVisbilityDefault = conf.get(ANNOTATION_SOURCE_VISIBILITY_DEFAULT, "");

        this.annotationIgnoreUnknownFields = conf.getBoolean(ANNOTATION_IGNORE_UNKNOWN_FIELDS, true);

        try {
            if (annotationRawTransformationEnabled) {
                String annotationRawTransformationConfig = conf.get(ANNOTATION_RAW_TRANSFORMATION_CONFIG, "");

                Processor processor = new Processor(false);
                XsltCompiler xsltCompiler = processor.newXsltCompiler();
                XsltExecutable xsltExecutable = xsltCompiler
                                .compile(new StreamSource(AnnotationHelper.class.getClassLoader().getResource(annotationRawTransformationConfig).openStream()));

                this.xslt30Transformer = xsltExecutable.load30();
            }
        } catch (IOException | SaxonApiException | FileSystemNotFoundException e) {
            log.error(e);
            throw new RuntimeException(e);
        }
    }

    /**
     * helper method for concatenating existing tableNames with the annotation property
     *
     * @param tableNames
     * @return
     */
    public String[] getAnnotationTableNames(String[] tableNames) {
        if (tableNames == null || tableNames.length == 0) {
            return new String[] {this.annotationTableName, this.annotationSourceTableName};
        } else {
            String[] annotationTableNames = new String[tableNames.length + 2];
            System.arraycopy(tableNames, 0, annotationTableNames, 0, tableNames.length);
            annotationTableNames[tableNames.length] = this.annotationTableName;
            annotationTableNames[tableNames.length + 1] = this.annotationSourceTableName;

            return annotationTableNames;
        }
    }

    /**
     * helper method for concatenating existing tableLoaderProperties with the annotation property
     *
     * @param tableLoaderPriorities
     * @return
     */
    public int[] getAnnotationTableLoaderPriorities(int[] tableLoaderPriorities) {
        if (tableLoaderPriorities == null || tableLoaderPriorities.length == 0) {
            return new int[] {this.annotationTableLoaderPriority, this.annotationSourceTableLoaderPriority};
        } else {
            int[] annotationTableLoaderPriorities = new int[tableLoaderPriorities.length + 2];
            System.arraycopy(tableLoaderPriorities, 0, annotationTableLoaderPriorities, 0, tableLoaderPriorities.length);
            annotationTableLoaderPriorities[tableLoaderPriorities.length] = this.annotationTableLoaderPriority;
            annotationTableLoaderPriorities[tableLoaderPriorities.length + 1] = this.annotationSourceTableLoaderPriority;

            return annotationTableLoaderPriorities;
        }
    }

    /**
     * to be used in processBulk() as a part of DataTypeHandler
     *
     * @param jsonInBytes
     * @param event
     * @param fields
     * @return
     */
    public Multimap<BulkIngestKey,Value> processBulk(byte[] jsonInBytes, RawRecordContainer event, Multimap<String,NormalizedContentInterface> fields) {
        Multimap<BulkIngestKey,Value> values = HashMultimap.create();

        try {
            Annotation annotation = buildAnnotation(jsonInBytes, event);
            AnnotationSerializer<Iterator<Map.Entry<Key,Value>>,Annotation> serializer = new AccumuloAnnotationSerializer();
            Iterator<Map.Entry<Key,Value>> annotationKeyIterator = serializer.serialize(annotation);

            while (annotationKeyIterator.hasNext()) {
                Map.Entry<Key,Value> entry = annotationKeyIterator.next();
                values.put(new BulkIngestKey(annotationTableNameText, entry.getKey()), entry.getValue());
            }

            AccumuloAnnotationSourceSerializer annotationSourceSerializer = new AccumuloAnnotationSourceSerializer();
            Iterator<Map.Entry<Key,Value>> annotationSourceKeyIterator = annotationSourceSerializer.serialize(annotation.getSource());

            while (annotationSourceKeyIterator.hasNext()) {
                Map.Entry<Key,Value> entry = annotationSourceKeyIterator.next();
                values.put(new BulkIngestKey(annotationSourceTableNameText, entry.getKey()), entry.getValue());
            }

        } catch (InvalidProtocolBufferException | AnnotationSerializationException | SaxonApiException e) {
            log.error(e);
            // failing the record
            throw new RuntimeException(e);
        }

        return values;
    }

    /**
     * to be used in process() as a part of ExtendedDataTypeHandler
     *
     * @param event
     * @param contextWriter
     * @param context
     * @param reporter
     * @param uid
     * @param visibility
     * @param shardId
     * @param rawData
     * @param <KEYOUT>
     * @param <VALUEOUT>
     * @param <KEYIN>
     * @throws IOException
     * @throws InterruptedException
     */
    public <KEYOUT,VALUEOUT,KEYIN> void process(RawRecordContainer event, ContextWriter<KEYOUT,VALUEOUT> contextWriter,
                    TaskInputOutputContext<KEYIN,? extends RawRecordContainer,KEYOUT,VALUEOUT> context, StatusReporter reporter, UID uid, byte[] visibility,
                    byte[] shardId, byte[] rawData) throws IOException, InterruptedException {
        try {
            Annotation annotation = buildAnnotation(rawData, shardId, uid, visibility, event);
            AnnotationSerializer<Iterator<Map.Entry<Key,Value>>,Annotation> annotationSerializer = new AccumuloAnnotationSerializer();
            Iterator<Map.Entry<Key,Value>> annotationKeyIterator = annotationSerializer.serialize(annotation);

            while (annotationKeyIterator.hasNext()) {
                Map.Entry<Key,Value> entry = annotationKeyIterator.next();
                contextWriter.write(new BulkIngestKey(annotationTableNameText, entry.getKey()), entry.getValue(), context);
            }

            AccumuloAnnotationSourceSerializer annotationSourceSerializer = new AccumuloAnnotationSourceSerializer();

            Iterator<Map.Entry<Key,Value>> annotationSourceKeyIterator = annotationSourceSerializer.serialize(annotation.getSource());

            while (annotationSourceKeyIterator.hasNext()) {
                Map.Entry<Key,Value> entry = annotationSourceKeyIterator.next();
                contextWriter.write(new BulkIngestKey(annotationSourceTableNameText, entry.getKey()), entry.getValue(), context);
            }
        } catch (InvalidProtocolBufferException | AnnotationSerializationException | SaxonApiException e) {
            log.error(e);
            // failing the record
            throw new RuntimeException(e);
        }
    }

    public Annotation buildAnnotation(byte[] jsonInBytes, RawRecordContainer event) throws InvalidProtocolBufferException, SaxonApiException {
        return buildAnnotation(jsonInBytes, event.getShardId().getBytes(), event.getId(), event.getVisibility().flatten(), event);
    }

    /**
     * this method parses jsonInBytes and sets the properties that are passed in. when there are conflicts between properties generated from ingest and json
     * source, it will preserve the parameters
     *
     * @param jsonInBytes
     * @param shardId
     * @param uid
     * @param visibility
     * @param event
     * @return
     * @throws InvalidProtocolBufferException
     * @throws SaxonApiException
     */
    public Annotation buildAnnotation(byte[] jsonInBytes, byte[] shardId, UID uid, byte[] visibility, RawRecordContainer event)
                    throws InvalidProtocolBufferException, SaxonApiException {
        // transform the json via configured xslt
        String jsonString = transformJson(jsonInBytes);

        Annotation.Builder annotationBuilder = Annotation.newBuilder();

        if (this.annotationIgnoreUnknownFields) {
            JsonFormat.parser().ignoringUnknownFields().merge(jsonString, annotationBuilder);
        } else {
            JsonFormat.parser().merge(jsonString, annotationBuilder);
        }

        Annotation.Builder datawaveAnnotationBuilder = Annotation.newBuilder();

        // populating DATAWAVE fields from event/fields
        datawaveAnnotationBuilder.setShard(new String(shardId));
        datawaveAnnotationBuilder.setDataType(event.getDataType().outputName());
        datawaveAnnotationBuilder.setUid(uid.toString());
        datawaveAnnotationBuilder.putMetadata("visibility", new String(visibility));
        datawaveAnnotationBuilder.putMetadata("created_date", DateHelper.format8601(new Date(event.getTimestamp())));

        // annotation source is required, but checking just in case
        if (annotationBuilder.getSource() != null) {
            if (annotationSourceVisibilityInheritEvent) {
                datawaveAnnotationBuilder.getSourceBuilder().putMetadata("visibility", new String(visibility));
            } else {
                datawaveAnnotationBuilder.getSourceBuilder().putMetadata("visibility", annotationSourceVisbilityDefault);
            }
            datawaveAnnotationBuilder.getSourceBuilder().putMetadata("created_date", DateHelper.format8601(new Date(event.getTimestamp())));
        }

        // datawaveAnnotation represents properties that are generated during ingest
        Annotation datawaveAnnotation = datawaveAnnotationBuilder.build();

        // when there are conflicts between properties generated from ingest and json source,
        // mergeFrom will collapse them in a way that preserves datawaveAnnotation
        return AnnotationUtils.injectAnnotationHash(annotationBuilder.mergeFrom(datawaveAnnotation).build());
    }

    /**
     * transform the json with provided xslt if it's configured
     *
     * @param jsonInBytes
     * @return
     * @throws SaxonApiException
     */
    protected String transformJson(byte[] jsonInBytes) throws SaxonApiException {
        String rawJsonString = new String(jsonInBytes);

        if (this.annotationRawTransformationEnabled) {
            Processor processor = new Processor(false);
            JsonBuilder jsonBuilder = processor.newJsonBuilder();
            ByteArrayOutputStream transformedJson = new ByteArrayOutputStream();

            // load the rawJsonString into generic saxon XdmValue object to apply transformation
            XdmValue jsonValue = jsonBuilder.parseJson(rawJsonString);

            // applying transformation
            xslt30Transformer.applyTemplates(jsonValue, xslt30Transformer.newSerializer(transformedJson));

            return transformedJson.toString();
        } else {
            return rawJsonString;
        }
    }
}

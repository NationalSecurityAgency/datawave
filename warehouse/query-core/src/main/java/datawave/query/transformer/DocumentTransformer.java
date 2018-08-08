package datawave.query.transformer;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import datawave.marking.MarkingFunctions;
import datawave.query.DocumentSerialization;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.Attributes;
import datawave.query.attributes.Content;
import datawave.query.attributes.Document;
import datawave.query.attributes.TimingMetadata;
import datawave.query.function.LogTiming;
import datawave.query.function.deserializer.DocumentDeserializer;
import datawave.query.iterator.QueryOptions;
import datawave.query.iterator.profile.QuerySpan;
import datawave.query.cardinality.CardinalityConfiguration;
import datawave.query.cardinality.CardinalityRecord;
import datawave.query.jexl.JexlASTHelper;
import datawave.util.StringUtils;
import datawave.util.time.DateHelper;
import datawave.webservice.query.Query;
import datawave.webservice.query.QueryImpl;
import datawave.webservice.query.QueryImpl.Parameter;
import datawave.webservice.query.exception.EmptyObjectException;
import datawave.webservice.query.logic.BaseQueryLogic;
import datawave.webservice.query.logic.WritesQueryMetrics;
import datawave.webservice.query.logic.WritesResultCardinalities;
import datawave.webservice.query.metric.BaseQueryMetric;
import datawave.webservice.query.result.event.EventBase;
import datawave.webservice.query.result.event.FieldBase;
import datawave.webservice.query.result.event.ResponseObjectFactory;
import datawave.webservice.query.result.event.Metadata;
import datawave.webservice.result.BaseQueryResponse;
import datawave.webservice.result.EventQueryResponseBase;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Transforms a document into a web service Event Object.
 *
 * Currently, this approach will support nested documents, but the nested attributes are planted in the flat structure using the name of that field from the
 * Document. Once we move toward a nested event, we can have a simpler approach.
 *
 */
public class DocumentTransformer extends EventQueryTransformer implements WritesQueryMetrics, WritesResultCardinalities {
    
    protected DocumentDeserializer deserializer;
    
    protected Boolean reducedResponse;
    
    protected List<String> contentFieldNames = Collections.emptyList();
    
    private static final Logger log = Logger.getLogger(DocumentTransformer.class);
    private static final Map<String,String> EMPTY_MARKINGS = new HashMap<>();
    private long sourceCount = 0;
    private long nextCount = 0;
    private long seekCount = 0;
    private long docRanges = 0;
    private long fiRanges = 0;
    private boolean logTimingDetails = false;
    private CardinalityRecord resultCardinalityDocumentDate = null;
    private CardinalityRecord resultCardinalityQueryDate = null;
    CardinalityConfiguration cardinalityConfiguration = null;
    private int objectsTransformed = 0;
    private long logicCreated = System.currentTimeMillis();
    private Set<String> projectFields = Collections.emptySet();
    private Set<String> blacklistedFields = Collections.emptySet();
    Map<String,List<String>> primaryToSecondaryFieldMap = Collections.emptyMap();
    
    /*
     * The 'HIT_TERM' feature required that an attribute value also contain the attribute's field name. The current implementation does it by prepending the
     * field name to the value with a colon separator, like so: BUDDY:fred. In the case where a data model has been applied to the query, the
     * DocumentTransformer will grab the prefix in this kind of field and transform it, so that '208_2.208.2.0:someplace.com' will be transformed to
     * 'URL_URL.208.2.0:someplace.com' In case there are ever any more fields to be treated this way, a transformValuePrefixFields collection will be used to
     * contain fields that should be transformed this way.
     */
    private static final String HIT_TERM = "HIT_TERM";
    
    private final Collection<String> transformValuePrefixFields = Sets.newHashSet(HIT_TERM);
    
    /**
     * By default, assume each cell still has the visibility attached to it
     *
     * @param logic
     * @param settings
     * @param responseObjectFactory
     */
    public DocumentTransformer(BaseQueryLogic<Entry<Key,Value>> logic, Query settings, MarkingFunctions markingFunctions,
                    ResponseObjectFactory responseObjectFactory) {
        this(logic, settings, markingFunctions, responseObjectFactory, false);
    }
    
    public DocumentTransformer(BaseQueryLogic<Entry<Key,Value>> logic, Query settings, MarkingFunctions markingFunctions,
                    ResponseObjectFactory responseObjectFactory, Boolean reducedResponse) {
        
        this(null != logic ? logic.getTableName() : null, settings, markingFunctions, responseObjectFactory, reducedResponse);
        this.logic = logic;
    }
    
    public DocumentTransformer(String tableName, Query settings, MarkingFunctions markingFunctions, ResponseObjectFactory responseObjectFactory,
                    Boolean reducedResponse) {
        super(tableName, settings, markingFunctions, responseObjectFactory);
        
        this.deserializer = DocumentSerialization.getDocumentDeserializer(settings);
        
        this.reducedResponse = reducedResponse;
        
        String logTimingDetailsStr = settings.findParameter(QueryOptions.LOG_TIMING_DETAILS).getParameterValue().trim();
        if (org.apache.commons.lang.StringUtils.isNotBlank(logTimingDetailsStr)) {
            logTimingDetails = Boolean.parseBoolean(logTimingDetailsStr);
        }
    }
    
    protected Map<String,String> getAdditionalCardinalityValues(Key documentKey, Document document) {
        Map<String,String> additionalValues = new HashMap<>();
        additionalValues.put("QUERY_USER", this.settings.getOwner());
        
        additionalValues.put("QUERY_LOGIC_NAME", this.settings.getQueryLogicName());
        
        long documentDate = document.getTimestamp();
        additionalValues.put("RESULT_DATA_AGE", Long.toString((logicCreated - documentDate) / 86400000));
        
        String[] cfSplit = documentKey.getColumnFamily().toString().split("\0");
        additionalValues.put("RESULT_DATATYPE", cfSplit[0]);
        
        return additionalValues;
    }
    
    // When a single Ivarator is used during a query on the teserver, we save time by not sorting the UIDs (not necessary for further comparisons).
    // To ensure that returned keys appear to be in sorted order on the way back we prpend a one-up number to the colFam.
    // In this edge case, the prepended number needs to be removed.
    static protected Key correctKey(Key origKey) {
        Key key = origKey;
        if (key != null) {
            String colFam = key.getColumnFamily().toString();
            String[] colFamParts = StringUtils.split(colFam, '\0');
            if (colFamParts.length == 3) {
                // skip part 0 and return a key with parts 1 & 2 as the colFam
                key = new Key(key.getRow(), new Text(colFamParts[1] + '\0' + colFamParts[2]), key.getColumnQualifier(), key.getColumnVisibility(),
                                key.getTimestamp());
            }
        }
        return key;
    }
    
    @Override
    public EventBase transform(Entry<?,?> input) {
        if (null == input)
            throw new IllegalArgumentException("Input cannot be null");
        
        EventBase output = null;
        
        Key documentKey = null;
        Document document = null;
        String dataType = null;
        String uid = null;
        
        @SuppressWarnings("unchecked")
        Entry<Key,org.apache.accumulo.core.data.Value> entry = (Entry<Key,org.apache.accumulo.core.data.Value>) input;
        
        Entry<Key,Document> documentEntry = deserializer.apply(entry);
        
        documentKey = correctKey(documentEntry.getKey());
        document = documentEntry.getValue();
        
        if (null == documentKey || null == document)
            throw new IllegalArgumentException("Null key or value. Key:" + documentKey + ", Value: " + entry.getValue());
        
        extractMetrics(document, documentKey);
        document.debugDocumentSize(documentKey);
        
        String row = documentKey.getRow().toString();
        
        String colf = documentKey.getColumnFamily().toString();
        
        int index = colf.indexOf("\0");
        Preconditions.checkArgument(-1 != index);
        
        dataType = colf.substring(0, index);
        uid = colf.substring(index + 1);
        
        // We don't have to consult the Document to rebuild the Visibility, the key
        // should have the correct top-level visibility
        ColumnVisibility eventCV = new ColumnVisibility(documentKey.getColumnVisibility());
        
        try {
            
            for (String contentFieldName : this.contentFieldNames) {
                if (document.containsKey(contentFieldName)) {
                    Attribute<?> contentField = document.remove(contentFieldName);
                    if (contentField.getData().toString().equalsIgnoreCase("true")) {
                        Content c = new Content(uid, contentField.getMetadata(), document.isToKeep());
                        document.put(contentFieldName, c, false, this.reducedResponse);
                    }
                }
            }
            
            for (String primaryField : this.primaryToSecondaryFieldMap.keySet()) {
                if (!document.containsKey(primaryField)) {
                    for (String secondaryField : this.primaryToSecondaryFieldMap.get(primaryField)) {
                        if (document.containsKey(secondaryField)) {
                            document.put(primaryField, document.get(secondaryField), false, this.reducedResponse);
                            break;
                        }
                    }
                }
            }
            
            // build response method here
            output = buildResponse(document, documentKey, eventCV, colf, row, this.markingFunctions);
            
        } catch (Exception ex) {
            log.error("Error building response document", ex);
            throw new RuntimeException(ex);
        }
        
        if (output == null) {
            // buildResponse will return a null object if there was only metadata in the document
            throw new EmptyObjectException();
        }
        
        if (cardinalityConfiguration != null) {
            collectCardinalities(document, documentKey, uid, dataType);
        }
        
        return output;
    }
    
    protected EventBase buildResponse(Document document, Key documentKey, ColumnVisibility eventCV, String colf, String row, MarkingFunctions mf)
                    throws MarkingFunctions.Exception {
        
        Map<String,String> markings = mf.translateFromColumnVisibility(eventCV);
        
        EventBase event = null;
        final Collection<FieldBase<?>> documentFields = buildDocumentFields(documentKey, null, document, eventCV, mf);
        // if documentFields is empty, then the response contained only timing metadata
        if (documentFields.size() > 0) {
            event = this.responseObjectFactory.getEvent();
            event.setMarkings(markings);
            event.setFields(new ArrayList<>(documentFields));
            
            Metadata metadata = new Metadata();
            String[] colfParts = StringUtils.split(colf, '\0');
            if (colfParts.length >= 1) {
                metadata.setDataType(colfParts[0]);
            }
            
            if (colfParts.length >= 2) {
                metadata.setInternalId(colfParts[1]);
            }
            
            if (this.tableName != null) {
                metadata.setTable(this.tableName);
            }
            metadata.setRow(row);
            event.setMetadata(metadata);
            
            if (eventQueryDataDecoratorTransformer != null) {
                event = (EventBase) eventQueryDataDecoratorTransformer.transform(event);
            }
            
            // assign an estimate of the event size based on the document size
            // in practice this is about 2.5 times the size of the document estimated size
            // we need to set something here for page size trigger purposes.
            event.setSizeInBytes(Math.round(document.sizeInBytes() * 2.5d));
        }
        
        return event;
    }
    
    /**
     * Builds the document's fields provided the given document key and the document itself.
     *
     * @param documentKey
     * @param document
     * @return
     */
    protected Collection<FieldBase<?>> buildDocumentFields(Key documentKey, String documentName, Document document, ColumnVisibility topLevelColumnVisibility,
                    MarkingFunctions markingFunctions) {
        
        // Whether the fields were added to projectFields or removed from blacklistedFields, they user does not want them returned
        // If neither a projection nor a blacklist was used then the suppressFields set should remain empty
        Set<String> suppressFields = Collections.emptySet();
        if (cardinalityConfiguration != null) {
            if (projectFields.size() > 0) {
                suppressFields = cardinalityConfiguration.getStoredProjectFieldsToAdd(getQm(), projectFields);
            } else if (blacklistedFields.size() > 0) {
                suppressFields = cardinalityConfiguration.getStoredBlacklistedFieldsToRemove(getQm(), blacklistedFields);
            }
        }
        
        Set<FieldBase<?>> Fields = new HashSet<>();
        final Map<String,Attribute<? extends Comparable<?>>> documentData = document.getDictionary();
        
        String fn = null;
        Attribute<?> attribute = null;
        for (Entry<String,Attribute<? extends Comparable<?>>> data : documentData.entrySet()) {
            
            // skip metadata fields
            if (data.getValue() instanceof datawave.query.attributes.Metadata) {
                continue;
            }
            fn = (documentName == null) ? data.getKey() : documentName;
            
            // Some fields were added by the queryPlanner. This will ensure that the original projectFields and blacklistFields are honored
            // remove any grouping context (only return the field up until the first dot)
            if (!suppressFields.contains(JexlASTHelper.removeGroupingContext(fn))) {
                // Apply the reverse mapping to make the field name human-readable again
                if (null != this.getQm()) {
                    fn = this.getQm().aliasFieldNameReverseModel(fn);
                }
                attribute = data.getValue();
                Fields.addAll(buildDocumentFields(documentKey, fn, attribute, topLevelColumnVisibility, markingFunctions));
            }
        }
        return Fields;
    }
    
    protected void extractMetrics(Document document, Key documentKey) {
        
        Map<String,Attribute<? extends Comparable<?>>> dictionary = document.getDictionary();
        Attribute<? extends Comparable<?>> timingMetadataAttribute = dictionary.get(LogTiming.TIMING_METADATA);
        if (timingMetadataAttribute != null && timingMetadataAttribute instanceof TimingMetadata) {
            TimingMetadata timingMetadata = (TimingMetadata) timingMetadataAttribute;
            long currentSourceCount = timingMetadata.getSourceCount();
            long currentNextCount = timingMetadata.getNextCount();
            long currentSeekCount = timingMetadata.getSeekCount();
            String host = timingMetadata.getHost();
            sourceCount += currentSourceCount;
            nextCount += currentNextCount;
            seekCount += currentSeekCount;
            Map<String,Long> stageTimers = timingMetadata.getStageTimers();
            if (stageTimers.containsKey(QuerySpan.Stage.DocumentSpecificTree.toString())) {
                docRanges++;
            } else if (stageTimers.containsKey(QuerySpan.Stage.FieldIndexTree.toString())) {
                fiRanges++;
            }
            
            if (logTimingDetails || log.isTraceEnabled()) {
                StringBuilder sb = new StringBuilder();
                sb.append("retrieved document from host:").append(host).append(" at key:").append(documentKey.toStringNoTime()).append(" stageTimers:")
                                .append(stageTimers.toString());
                sb.append(" sourceCount:").append(currentSourceCount).append(" nextCount:").append(currentNextCount).append(" seekCount:")
                                .append(currentSeekCount);
                if (log.isTraceEnabled()) {
                    log.trace(sb.toString());
                } else {
                    log.info(sb.toString());
                }
            }
            if (dictionary.size() == 1) {
                // this document contained only timing metadata
                throw new EmptyObjectException();
            }
        }
    }
    
    public void writeQueryMetrics(BaseQueryMetric metric) {
        
        // if any timing details have been returned, add metrics
        if (sourceCount > 0) {
            metric.setSourceCount(sourceCount);
            metric.setNextCount(nextCount);
            metric.setSeekCount(seekCount);
            metric.setDocRanges(docRanges);
            metric.setFiRanges(fiRanges);
        }
    }
    
    protected List<String> getFieldValues(Document document, String field, boolean shortCircuit) {
        
        Map<String,String> reverseModel = cardinalityConfiguration.getCardinalityFieldReverseMapping();
        List<String> valueList = new ArrayList<>();
        
        for (Map.Entry<String,Attribute<? extends Comparable<?>>> e : document.getDictionary().entrySet()) {
            String docField = e.getKey();
            String baseDocField = JexlASTHelper.removeGroupingContext(docField);
            String reverseMappedField = reverseModel.containsKey(baseDocField) ? reverseModel.get(baseDocField) : "";
            if (baseDocField.equals(field) || reverseMappedField.equals(field)) {
                Attribute<?> a = e.getValue();
                if (a instanceof Attributes) {
                    for (Attribute<?> attr : ((Attributes) a).getAttributes()) {
                        valueList.add(attr.getData().toString());
                    }
                } else {
                    valueList.add(a.getData().toString());
                }
                if (shortCircuit) {
                    break;
                }
            }
        }
        return valueList;
    }
    
    protected void collectCardinalities(Document document, Key documentKey, String uid, String dataType) {
        
        // record result cardinality
        Map<String,String> additionalValues = getAdditionalCardinalityValues(documentKey, document);
        
        String eventId = uid;
        String uidField = cardinalityConfiguration.getCardinalityUidField();
        if (org.apache.commons.lang.StringUtils.isNotBlank(uidField)) {
            List<String> documentUidValues = getFieldValues(document, uidField, true);
            if (documentUidValues.isEmpty() == false) {
                eventId = documentUidValues.get(0);
            }
        }
        
        Map<String,List<String>> valueMap = new HashMap<>();
        Set<String> allFieldNames = cardinalityConfiguration.getAllFieldNames();
        for (String f : allFieldNames) {
            if (additionalValues.containsKey(f)) {
                List<String> fieldValues = new ArrayList<>();
                fieldValues.add(additionalValues.get(f));
                valueMap.put(f, fieldValues);
            } else {
                List<String> fieldValues = getFieldValues(document, f, false);
                valueMap.put(f, fieldValues);
            }
        }
        
        Date dataDate = null;
        long timestamp = document.getTimestamp();
        if (timestamp == Long.MAX_VALUE) {
            String row = documentKey.getRow().toString();
            dataDate = DateHelper.parseWithGMT(row);
            if (log.isTraceEnabled()) {
                log.trace("Document.getTimestamp() returned Log.MAX_VALUE - " + documentKey.toString() + " - computed dataDate from row: "
                                + dataDate.toString());
            }
        } else {
            dataDate = new Date(timestamp);
        }
        
        resultCardinalityDocumentDate.addEntry(valueMap, eventId, dataType, dataDate);
        resultCardinalityQueryDate.addEntry(valueMap, eventId, dataType, dataDate);
        objectsTransformed++;
        if (objectsTransformed >= cardinalityConfiguration.getFlushThreshold()) {
            writeResultCardinalities();
            objectsTransformed = 0;
        }
    }
    
    @Override
    public void writeResultCardinalities() {
        if (cardinalityConfiguration != null) {
            if (resultCardinalityDocumentDate != null && resultCardinalityQueryDate != null) {
                try {
                    File directory = new File(cardinalityConfiguration.getOutputFileDirectory());
                    if (!directory.exists()) {
                        directory.mkdir();
                    }
                    if (!directory.exists() || !directory.canWrite()) {
                        throw new IOException("Can not write to directory " + directory.getAbsolutePath());
                    }
                    
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss.SSS");
                    String date = sdf.format(new Date());
                    String queryId = this.settings.getId().toString();
                    
                    if (resultCardinalityDocumentDate.getNumEntries() > 0) {
                        File f1 = new File(directory, "cardinality-" + date + "-document-" + queryId + ".obj");
                        resultCardinalityDocumentDate.flushToDisk(f1);
                    }
                    if (resultCardinalityQueryDate.getNumEntries() > 0) {
                        File f2 = new File(directory, "cardinality-" + date + "-query-" + queryId + ".obj");
                        resultCardinalityQueryDate.flushToDisk(f2);
                    }
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }
            }
        }
    }
    
    /**
     * Accepts an attribute. The document data will be placed into the value of the Field.
     *
     * @param documentKey
     * @return
     */
    protected Collection<FieldBase<?>> buildDocumentFields(Key documentKey, String fieldName, Attribute<?> attr, ColumnVisibility topLevelColumnVisibility,
                    MarkingFunctions markingFunctions) {
        
        Set<FieldBase<?>> myFields = new HashSet<>();
        
        if (attr instanceof Attributes) {
            Attributes attributeList = Attributes.class.cast(attr);
            
            for (Attribute<? extends Comparable<?>> embeddedAttr : attributeList.getAttributes())
                myFields.addAll(buildDocumentFields(documentKey, fieldName, embeddedAttr, topLevelColumnVisibility, markingFunctions));
            
        } else {
            // Use the markings on the Field if we're returning the markings to the client
            if (!this.reducedResponse) {
                try {
                    Map<String,String> markings = markingFunctions.translateFromColumnVisibility(attr.getColumnVisibility());
                    FieldBase<?> field = this.makeField(fieldName, markings, attr.getColumnVisibility(), attr.getTimestamp(), attr.getData());
                    MarkingFunctions.Util.populate(field, markings);
                    myFields.add(field);
                } catch (Exception ex) {
                    log.error("unable to process markings:" + ex);
                }
            } else {
                // noinspection RedundantCast
                FieldBase<?> f = createField(fieldName, (Long) null, attr, EMPTY_MARKINGS, (String) null);
                myFields.add(f);
            }
        }
        
        return myFields;
    }
    
    /**
     * Helper method to create a field for a given attribute.
     *
     * @param fieldName
     * @param ts
     * @param attribute
     * @return
     */
    protected FieldBase<?> createField(final String fieldName, final long ts, final Attribute<?> attribute, Map<String,String> markings, String columnVisibility) {
        
        if (markings == null || markings.isEmpty()) {
            log.warn("Null or empty markings for " + fieldName + ":" + attribute);
        }
        
        return createField(fieldName, (Long) ts, attribute, markings, columnVisibility);
    }
    
    protected FieldBase<?> createField(final String fieldName, final Long ts, final Attribute<?> attribute, Map<String,String> markings, String columnVisibility) {
        if (this.transformValuePrefixFields.contains(fieldName)) {
            return convertHitTermField(fieldName, ts, attribute, markings, columnVisibility);
        }
        return this.makeField(fieldName, markings, columnVisibility, ts, attribute.getData());
    }
    
    private FieldBase<?> convertHitTermField(final String fieldName, final Long ts, Attribute<?> attribute, Map<String,String> markings, String columnVisibility) {
        return this.makeField(fieldName, markings, columnVisibility, ts, convertMappedAttribute(attribute).getData());
    }
    
    private Attribute<?> convertMappedAttribute(Attribute<?> attribute) {
        String attributeString = attribute.getData().toString();
        int idx = attributeString.indexOf(':');
        if (idx != -1) {
            String firstPart = attributeString.substring(0, idx);
            String secondPart = attributeString.substring(idx);
            // Apply the reverse mapping to make the field name human-readable again
            if (null != this.getQm()) {
                firstPart = this.getQm().aliasFieldNameReverseModel(firstPart);
            }
            attribute = new Content(firstPart + secondPart, attribute.getMetadata(), attribute.isToKeep());
        }
        return attribute;
    }
    
    @Override
    public BaseQueryResponse createResponse(List<Object> resultList) {
        EventQueryResponseBase response = this.responseObjectFactory.getEventQueryResponse();
        List<EventBase> eventList = Lists.newArrayListWithCapacity(resultList.size());
        Set<String> fieldSet = Sets.newTreeSet();
        for (Object o : resultList) {
            EventBase<?,?> e = (EventBase<?,?>) o;
            for (FieldBase<?> f : e.getFields()) {
                fieldSet.add(f.getName());
            }
            eventList.add(e);
            
        }
        
        response.setFields(Lists.newArrayList(fieldSet));
        
        response.setEvents(eventList);
        response.setReturnedEvents((long) eventList.size());
        
        return response;
    }
    
    @Override
    public EventQueryDataDecoratorTransformer getEventQueryDataDecoratorTransformer() {
        return eventQueryDataDecoratorTransformer;
    }
    
    @Override
    public void setEventQueryDataDecoratorTransformer(EventQueryDataDecoratorTransformer eventQueryDataDecoratorTransformer) {
        this.eventQueryDataDecoratorTransformer = eventQueryDataDecoratorTransformer;
        
        Set<Parameter> parameters = this.settings.getParameters();
        if (eventQueryDataDecoratorTransformer != null && parameters != null) {
            List<String> requestedDecorators = new ArrayList<>();
            for (QueryImpl.Parameter p : parameters) {
                if (p.getParameterName().equals("data.decorators")) {
                    String decoratorString = p.getParameterValue();
                    if (decoratorString != null) {
                        requestedDecorators.addAll(Arrays.asList(decoratorString.split(",")));
                        this.eventQueryDataDecoratorTransformer.setRequestedDecorators(requestedDecorators);
                    }
                }
            }
        }
    }
    
    @Override
    public List<String> getContentFieldNames() {
        return contentFieldNames;
    }
    
    @Override
    public void setContentFieldNames(List<String> contentFieldNames) {
        this.contentFieldNames = contentFieldNames;
    }
    
    public void setLogTimingDetails(Boolean logTimingDetails) {
        this.logTimingDetails = logTimingDetails;
    }
    
    public CardinalityConfiguration getCardinalityConfiguration() {
        return cardinalityConfiguration;
    }
    
    public void setCardinalityConfiguration(CardinalityConfiguration cardinalityConfiguration) {
        this.cardinalityConfiguration = cardinalityConfiguration;
        try {
            if (cardinalityConfiguration != null) {
                resultCardinalityDocumentDate = new CardinalityRecord(cardinalityConfiguration.getCardinalityFields(), CardinalityRecord.DateType.DOCUMENT);
                resultCardinalityQueryDate = new CardinalityRecord(cardinalityConfiguration.getCardinalityFields(), CardinalityRecord.DateType.CURRENT);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }
    
    public void setProjectFields(Set<String> projectFields) {
        this.projectFields = projectFields;
    }
    
    public void setBlacklistedFields(Set<String> blacklistedFields) {
        this.blacklistedFields = blacklistedFields;
    }
    
    public void setPrimaryToSecondaryFieldMap(Map<String,List<String>> primaryToSecondaryFieldMap) {
        this.primaryToSecondaryFieldMap = primaryToSecondaryFieldMap;
    }
}

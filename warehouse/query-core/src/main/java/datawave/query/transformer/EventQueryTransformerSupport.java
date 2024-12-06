package datawave.query.transformer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.log4j.Logger;

import com.esotericsoftware.kryo.Kryo;
import com.google.common.collect.Lists;

import datawave.core.query.cachedresults.CacheableLogic;
import datawave.core.query.exception.EmptyObjectException;
import datawave.core.query.logic.BaseQueryLogic;
import datawave.core.query.logic.BaseQueryLogicTransformer;
import datawave.core.query.logic.WritesQueryMetrics;
import datawave.marking.MarkingFunctions;
import datawave.microservice.query.Query;
import datawave.microservice.query.QueryImpl.Parameter;
import datawave.microservice.querymetric.BaseQueryMetric;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.Document;
import datawave.query.attributes.TimingMetadata;
import datawave.query.function.LogTiming;
import datawave.query.iterator.QueryOptions;
import datawave.query.iterator.profile.QuerySpan;
import datawave.query.model.QueryModel;
import datawave.query.parser.EventFields;
import datawave.webservice.query.cachedresults.CacheableQueryRow;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.query.result.event.EventBase;
import datawave.webservice.query.result.event.FieldBase;
import datawave.webservice.query.result.event.Metadata;
import datawave.webservice.query.result.event.ResponseObjectFactory;
import datawave.webservice.result.BaseQueryResponse;
import datawave.webservice.result.EventQueryResponseBase;

public abstract class EventQueryTransformerSupport<I,O> extends BaseQueryLogicTransformer<I,O> implements CacheableLogic, WritesQueryMetrics {

    protected EventFields eventFields = new EventFields();

    protected Kryo kryo = new Kryo();

    protected Query settings = null;

    protected BaseQueryLogic<Entry<Key,Value>> logic = null;

    protected Authorizations auths = null;

    protected EventQueryDataDecoratorTransformer eventQueryDataDecoratorTransformer = null;

    protected List<String> contentFieldNames = Collections.emptyList();

    protected static final Logger log = Logger.getLogger(EventQueryTransformerSupport.class);

    protected QueryModel qm;
    protected String tableName;
    protected ResponseObjectFactory responseObjectFactory;
    private long sourceCount = 0;
    private long nextCount = 0;
    private long seekCount = 0;
    private long yieldCount = 0L;
    private long docRanges = 0;
    private long fiRanges = 0;
    private boolean logTimingDetails = false;

    public EventQueryTransformerSupport(String tableName, Query settings, MarkingFunctions markingFunctions, ResponseObjectFactory responseObjectFactory) {
        super(markingFunctions);
        this.settings = settings;
        this.auths = new Authorizations(settings.getQueryAuthorizations().split(","));
        this.tableName = tableName;
        this.responseObjectFactory = responseObjectFactory;
        String logTimingDetailsStr = settings.findParameter(QueryOptions.LOG_TIMING_DETAILS).getParameterValue().trim();
        if (org.apache.commons.lang.StringUtils.isNotBlank(logTimingDetailsStr)) {
            logTimingDetails = Boolean.parseBoolean(logTimingDetailsStr);
        }
    }

    public EventQueryTransformerSupport(BaseQueryLogic<Entry<Key,Value>> logic, Query settings, MarkingFunctions markingFunctions,
                    ResponseObjectFactory responseObjectFactory) {
        this(logic.getTableName(), settings, markingFunctions, responseObjectFactory);
        this.logic = logic;
        this.responseObjectFactory = responseObjectFactory;
    }

    protected ResponseObjectFactory getResponseObjectFactory() {
        return this.responseObjectFactory;
    }

    protected Authorizations getAuths() {
        return this.auths;
    }

    @Override
    public CacheableQueryRow writeToCache(Object o) throws QueryException {
        EventBase<?,?> event = (EventBase<?,?>) o;

        CacheableQueryRow cqo = this.responseObjectFactory.getCacheableQueryRow();
        cqo.setMarkingFunctions(this.markingFunctions);
        Metadata metadata = event.getMetadata();
        cqo.setColFam(metadata.getDataType() + ":" + cqo.getEventId());
        cqo.setDataType(metadata.getDataType());
        cqo.setEventId(metadata.getInternalId());
        cqo.setRow(metadata.getRow());

        List<? extends FieldBase<?>> fields = event.getFields();
        for (FieldBase<?> f : fields) {
            cqo.addColumn(f.getName(), f.getTypedValue(), f.getMarkings(), f.getColumnVisibility(), f.getTimestamp());
        }
        return cqo;
    }

    @Override
    public Object readFromCache(CacheableQueryRow cacheableQueryRow) {
        Map<String,String> markings = cacheableQueryRow.getMarkings();
        String dataType = cacheableQueryRow.getDataType();
        String internalId = cacheableQueryRow.getEventId();
        String row = cacheableQueryRow.getRow();

        EventBase event = this.responseObjectFactory.getEvent();
        event.setMarkings(markings);

        Metadata metadata = new Metadata();
        metadata.setDataType(dataType);
        metadata.setInternalId(internalId);
        metadata.setRow(row);
        metadata.setTable(logic.getTableName());
        event.setMetadata(metadata);

        List<FieldBase<?>> fieldList = new ArrayList<>();
        Map<String,String> columnValueMap = cacheableQueryRow.getColumnValues();
        for (Entry<String,String> entry : columnValueMap.entrySet()) {
            String columnName = entry.getKey();
            String columnValue = entry.getValue();
            Map<String,String> columnMarkings = cacheableQueryRow.getColumnMarkings(columnName);
            String columnVisibility = cacheableQueryRow.getColumnVisibility(columnName);
            Long columnTimestamp = cacheableQueryRow.getColumnTimestamp(columnName);
            FieldBase<?> field = this.makeField(columnName, columnMarkings, columnVisibility, columnTimestamp, columnValue);
            fieldList.add(field);
        }
        event.setFields(fieldList);
        return event;
    }

    @Override
    public BaseQueryResponse createResponse(List<Object> resultList) {
        EventQueryResponseBase response = this.responseObjectFactory.getEventQueryResponse();
        List<EventBase> eventList = new ArrayList<>();
        Set<String> fieldSet = new TreeSet<>();
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

    protected FieldBase<?> makeField(String name, Map<String,String> markings, String columnVisibility, Long timestamp, Object value) {
        FieldBase<?> field = this.responseObjectFactory.getField();
        field.setName(name);
        field.setMarkings(markings);
        field.setColumnVisibility(columnVisibility);
        field.setTimestamp(timestamp);
        field.setValue(value);
        return field;
    }

    protected FieldBase<?> makeField(String name, Map<String,String> markings, ColumnVisibility columnVisibility, Long timestamp, Object value) {
        FieldBase<?> field = makeField(name, markings, (String) null, timestamp, value);
        field.setColumnVisibility(columnVisibility);
        return field;
    }

    public EventQueryDataDecoratorTransformer getEventQueryDataDecoratorTransformer() {
        return eventQueryDataDecoratorTransformer;
    }

    public void setEventQueryDataDecoratorTransformer(EventQueryDataDecoratorTransformer eventQueryDataDecoratorTransformer) {
        this.eventQueryDataDecoratorTransformer = eventQueryDataDecoratorTransformer;

        Set<Parameter> parameters = this.settings.getParameters();
        if (eventQueryDataDecoratorTransformer != null && parameters != null) {
            List<String> requestedDecorators = new ArrayList<>();
            for (Parameter p : parameters) {
                if (p.getParameterName().equals("data.decorators")) {
                    String decoratorString = p.getParameterValue();
                    if (decoratorString != null) {
                        requestedDecorators.addAll(Arrays.asList(decoratorString.split(",")));
                        this.eventQueryDataDecoratorTransformer.setRequestedDecorators(requestedDecorators);
                    }
                }
            }
            // Ensure that the requested EventQueryDataDecorator instances have non-null ResponseObjectFactory
            // Otherwise, NPE will ensue...
            if (!requestedDecorators.isEmpty() && this.eventQueryDataDecoratorTransformer.getDataDecorators() != null) {
                for (String requestedDecorator : requestedDecorators) {
                    if (this.eventQueryDataDecoratorTransformer.getDataDecorators().containsKey(requestedDecorator)) {
                        EventQueryDataDecorator edd = this.eventQueryDataDecoratorTransformer.getDataDecorators().get(requestedDecorator);
                        if (edd.getResponseObjectFactory() == null) {
                            edd.setResponseObjectFactory(this.responseObjectFactory);
                        }
                    }
                }
            }
        }
    }

    public List<String> getContentFieldNames() {
        return contentFieldNames;
    }

    public void setContentFieldNames(List<String> contentFieldNames) {
        this.contentFieldNames = contentFieldNames;
    }

    public QueryModel getQm() {
        return qm;
    }

    public void setQm(QueryModel qm) {
        this.qm = qm;
    }

    protected void extractMetrics(Document document, Key documentKey) {

        Map<String,Attribute<? extends Comparable<?>>> dictionary = document.getDictionary();
        Attribute<? extends Comparable<?>> timingMetadataAttribute = dictionary.get(LogTiming.TIMING_METADATA);
        if (timingMetadataAttribute != null && timingMetadataAttribute instanceof TimingMetadata) {
            TimingMetadata timingMetadata = (TimingMetadata) timingMetadataAttribute;
            long currentSourceCount = timingMetadata.getSourceCount();
            long currentNextCount = timingMetadata.getNextCount();
            long currentSeekCount = timingMetadata.getSeekCount();
            long currentYieldCount = timingMetadata.getYieldCount();
            String host = timingMetadata.getHost();
            sourceCount += currentSourceCount;
            nextCount += currentNextCount;
            seekCount += currentSeekCount;
            yieldCount += currentYieldCount;
            Map<String,Long> stageTimers = timingMetadata.getStageTimers();
            if (stageTimers.containsKey(QuerySpan.Stage.DocumentSpecificTree.toString())) {
                docRanges++;
            } else if (stageTimers.containsKey(QuerySpan.Stage.FieldIndexTree.toString())) {
                fiRanges++;
            }

            if (logTimingDetails || log.isTraceEnabled()) {
                StringBuilder sb = new StringBuilder();
                sb.append("retrieved document from host:").append(host).append(" at key:").append(documentKey.toStringNoTime()).append(" stageTimers:")
                                .append(stageTimers);
                sb.append(" sourceCount:").append(currentSourceCount).append(" nextCount:").append(currentNextCount).append(" seekCount:")
                                .append(currentSeekCount).append(" yieldCount:").append(currentYieldCount);
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

    @Override
    public boolean hasMetrics() {
        return sourceCount + nextCount + seekCount + yieldCount + docRanges + fiRanges > 0;
    }

    @Override
    public long getSourceCount() {
        return sourceCount;
    }

    @Override
    public long getNextCount() {
        return nextCount;
    }

    @Override
    public long getSeekCount() {
        return seekCount;
    }

    @Override
    public long getYieldCount() {
        return yieldCount;
    }

    @Override
    public long getDocRanges() {
        return docRanges;
    }

    @Override
    public long getFiRanges() {
        return fiRanges;
    }

    @Override
    public void writeQueryMetrics(BaseQueryMetric metric) {

        // if any timing details have been returned, add metrics
        if (hasMetrics()) {
            metric.setSourceCount(sourceCount);
            metric.setNextCount(nextCount);
            metric.setSeekCount(seekCount);
            metric.setYieldCount(yieldCount);
            metric.setDocRanges(docRanges);
            metric.setFiRanges(fiRanges);
        }
    }

    @Override
    public void resetMetrics() {
        sourceCount = 0;
        nextCount = 0;
        seekCount = 0;
        yieldCount = 0;
        docRanges = 0;
        fiRanges = 0;
    }

}

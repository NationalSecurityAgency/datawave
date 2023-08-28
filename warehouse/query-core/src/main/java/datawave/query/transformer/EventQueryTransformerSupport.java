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

import datawave.marking.MarkingFunctions;
import datawave.query.model.QueryModel;
import datawave.query.parser.EventFields;
import datawave.webservice.query.Query;
import datawave.webservice.query.QueryImpl.Parameter;
import datawave.webservice.query.cachedresults.CacheableLogic;
import datawave.webservice.query.cachedresults.CacheableQueryRow;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.query.logic.BaseQueryLogic;
import datawave.webservice.query.logic.BaseQueryLogicTransformer;
import datawave.webservice.query.result.event.EventBase;
import datawave.webservice.query.result.event.FieldBase;
import datawave.webservice.query.result.event.Metadata;
import datawave.webservice.query.result.event.ResponseObjectFactory;
import datawave.webservice.result.BaseQueryResponse;
import datawave.webservice.result.EventQueryResponseBase;

public abstract class EventQueryTransformerSupport<I,O> extends BaseQueryLogicTransformer<I,O> implements CacheableLogic {

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

    public EventQueryTransformerSupport(String tableName, Query settings, MarkingFunctions markingFunctions, ResponseObjectFactory responseObjectFactory) {
        super(markingFunctions);
        this.settings = settings;
        this.auths = new Authorizations(settings.getQueryAuthorizations().split(","));
        this.tableName = tableName;
        this.responseObjectFactory = responseObjectFactory;
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
    public List<CacheableQueryRow> writeToCache(Object o) throws QueryException {

        List<CacheableQueryRow> cqoList = new ArrayList<>();
        EventBase<?,?> event = (EventBase<?,?>) o;

        CacheableQueryRow cqo = this.responseObjectFactory.getCacheableQueryRow();
        Metadata metadata = event.getMetadata();
        cqo.setColFam(metadata.getDataType() + ":" + cqo.getEventId());
        cqo.setDataType(metadata.getDataType());
        cqo.setEventId(metadata.getInternalId());
        cqo.setRow(metadata.getRow());

        List<? extends FieldBase<?>> fields = event.getFields();
        for (FieldBase<?> f : fields) {
            cqo.addColumn(f.getName(), f.getTypedValue(), f.getMarkings(), f.getColumnVisibility(), f.getTimestamp());
        }
        cqoList.add(cqo);
        return cqoList;
    }

    @Override
    public List<Object> readFromCache(List<CacheableQueryRow> cacheableQueryRowList) {

        List<Object> eventList = new ArrayList<>();

        for (CacheableQueryRow cqr : cacheableQueryRowList) {
            Map<String,String> markings = cqr.getMarkings();
            String dataType = cqr.getDataType();
            String internalId = cqr.getEventId();
            String row = cqr.getRow();

            EventBase event = this.responseObjectFactory.getEvent();
            event.setMarkings(markings);

            Metadata metadata = new Metadata();
            metadata.setDataType(dataType);
            metadata.setInternalId(internalId);
            metadata.setRow(row);
            metadata.setTable(logic.getTableName());
            event.setMetadata(metadata);

            List<FieldBase<?>> fieldList = new ArrayList<>();
            Map<String,String> columnValueMap = cqr.getColumnValues();
            for (Entry<String,String> entry : columnValueMap.entrySet()) {
                String columnName = entry.getKey();
                String columnValue = entry.getValue();
                Map<String,String> columnMarkings = cqr.getColumnMarkings(columnName);
                String columnVisibility = cqr.getColumnVisibility(columnName);
                Long columnTimestamp = cqr.getColumnTimestamp(columnName);
                FieldBase<?> field = this.makeField(columnName, columnMarkings, columnVisibility, columnTimestamp, columnValue);
                fieldList.add(field);
            }
            event.setFields(fieldList);
            eventList.add(event);
        }

        return eventList;
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

}

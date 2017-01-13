package nsa.datawave.query.transformer;

import java.io.ByteArrayInputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;

import nsa.datawave.marking.MarkingFunctions;
import nsa.datawave.marking.MarkingFunctions.Exception;
import nsa.datawave.query.model.QueryModel;
import nsa.datawave.query.parser.EventFields;
import nsa.datawave.query.parser.EventFields.FieldValue;
import nsa.datawave.query.rewrite.Constants;
import nsa.datawave.util.StringUtils;
import nsa.datawave.webservice.query.Query;
import nsa.datawave.webservice.query.QueryImpl;
import nsa.datawave.webservice.query.QueryImpl.Parameter;
import nsa.datawave.webservice.query.cachedresults.CacheableLogic;
import nsa.datawave.webservice.query.cachedresults.CacheableQueryRow;
import nsa.datawave.webservice.query.exception.QueryException;
import nsa.datawave.webservice.query.logic.BaseQueryLogic;
import nsa.datawave.webservice.query.logic.BaseQueryLogicTransformer;
import nsa.datawave.webservice.query.result.event.EventBase;
import nsa.datawave.webservice.query.result.event.ResponseObjectFactory;
import nsa.datawave.webservice.query.result.event.FieldBase;
import nsa.datawave.webservice.query.result.event.Metadata;
import nsa.datawave.webservice.result.BaseQueryResponse;
import nsa.datawave.webservice.result.EventQueryResponseBase;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.log4j.Logger;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.google.common.collect.Lists;

public class EventQueryTransformer extends BaseQueryLogicTransformer implements CacheableLogic {
    
    protected EventFields eventFields = new EventFields();
    
    protected Kryo kryo = new Kryo();
    
    protected Query settings = null;
    
    protected BaseQueryLogic<Entry<Key,Value>> logic = null;
    
    protected Authorizations auths = null;
    
    protected EventQueryDataDecoratorTransformer eventQueryDataDecoratorTransformer = null;
    
    protected List<String> contentFieldNames = Collections.emptyList();
    
    protected static Logger log = Logger.getLogger(EventQueryTransformer.class);
    
    protected QueryModel qm;
    protected String tableName;
    protected ResponseObjectFactory responseObjectFactory;
    
    public EventQueryTransformer(String tableName, Query settings, MarkingFunctions markingFunctions, ResponseObjectFactory responseObjectFactory) {
        super(markingFunctions);
        this.settings = settings;
        this.auths = new Authorizations(settings.getQueryAuthorizations().split(","));
        this.tableName = tableName;
        this.responseObjectFactory = responseObjectFactory;
    }
    
    public EventQueryTransformer(BaseQueryLogic<Entry<Key,Value>> logic, Query settings, MarkingFunctions markingFunctions,
                    ResponseObjectFactory responseObjectFactory) {
        this(logic.getTableName(), settings, markingFunctions, responseObjectFactory);
        this.logic = logic;
        this.responseObjectFactory = responseObjectFactory;
    }
    
    @Override
    public Object transform(Object input) {
        if (input instanceof Entry<?,?>) {
            @SuppressWarnings("unchecked")
            Entry<Key,org.apache.accumulo.core.data.Value> entry = (Entry<Key,org.apache.accumulo.core.data.Value>) input;
            
            Key key = entry.getKey();
            Value val = entry.getValue();
            if (entry.getKey() == null && entry.getValue() == null)
                return null;
            
            if (null == entry.getKey() || null == entry.getValue()) {
                throw new IllegalArgumentException("Null key or value. Key:" + entry.getKey() + ", Value: " + entry.getValue());
            }
            EventBase event = this.responseObjectFactory.getEvent();
            
            Map<String,String> markings = null;
            try {
                markings = this.markingFunctions.translateFromColumnVisibilityForAuths(new ColumnVisibility(key.getColumnVisibility()), this.auths);
            } catch (Exception e) {
                log.error("could not translate " + key.getColumnVisibility() + " to markings, skipping entry");
                return null;
            }
            if (null == markings || markings.isEmpty()) {
                // can't process this one because we did not have valid security markings
                log.error("Transformer visibility interpreter was null, skipping entry");
                return null;
            }
            ByteArrayInputStream bais = new ByteArrayInputStream(entry.getValue().get());
            Input i = new Input(bais);
            eventFields = kryo.readObject(i, EventFields.class);
            
            i.close();
            
            String row = entry.getKey().getRow().toString();
            String colf = entry.getKey().getColumnFamily().toString();
            String colq = entry.getKey().getColumnQualifier().toString();
            // if the column qualifier is set, then we have returned an alternate event from the one that was
            // evaluated by using the returnUidMapper (@see nsa.datawave.core.iterators.EvaluatingIterator: aggregateAltEvent)
            if (!colq.equals("")) {
                colf = colq;
            }
            int sepIndex = colf.indexOf(Constants.NULL_BYTE_STRING);
            String baseUid = colf.substring(sepIndex + 1);
            
            Set<FieldBase<?>> values = new HashSet<>();
            
            String origFieldName = null;
            String fieldName = null;
            
            // Hold unique Column Visibilities and merge them at the end
            // for the overall event ColumnVisibility.
            Set<ColumnVisibility> visibilitiesToMerge = new HashSet<>();
            
            for (Entry<String,Collection<FieldValue>> e : eventFields.asMap().entrySet()) {
                origFieldName = e.getKey();
                if (this.qm != null) {
                    fieldName = this.qm.aliasFieldNameReverseModel(origFieldName);
                } else {
                    fieldName = origFieldName;
                }
                
                for (FieldValue fv : e.getValue()) {
                    visibilitiesToMerge.add(fv.getVisibility());
                    try {
                        Map<String,String> fieldMarkings = this.markingFunctions.translateFromColumnVisibility(fv.getVisibility());
                        String value = new String(fv.getValue(), Charset.forName("UTF-8"));
                        // if this is a content field name, then replace the value with the uid
                        if (getContentFieldNames().contains(fieldName)) {
                            value = baseUid;
                        }
                        values.add(this.makeField(fieldName, fieldMarkings, new String(fv.getVisibility().getExpression()), entry.getKey().getTimestamp(),
                                        value));
                    } catch (Exception e1) {
                        throw new RuntimeException("could not make markings from: " + fv.getVisibility());
                    }
                }
            }
            
            ColumnVisibility columnVisibility = null;
            try {
                columnVisibility = this.markingFunctions.combine(visibilitiesToMerge);
                event.setMarkings(this.markingFunctions.translateFromColumnVisibility(columnVisibility));
            } catch (Exception e1) {
                throw new RuntimeException("could not make markings from: " + columnVisibility);
            }
            event.setFields(new ArrayList<>(values));
            
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
                event = (EventBase<?,?>) eventQueryDataDecoratorTransformer.transform(event);
            }
            
            // assign an estimate of the event size
            // in practice this is about 6 times the size of the kryo bytes
            event.setSizeInBytes(entry.getValue().getSize() * 6);
            
            return event;
        } else {
            throw new IllegalArgumentException("Invalid input type: " + input.getClass());
        }
        
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
            for (Map.Entry<String,String> entry : columnValueMap.entrySet()) {
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
            for (QueryImpl.Parameter p : parameters) {
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

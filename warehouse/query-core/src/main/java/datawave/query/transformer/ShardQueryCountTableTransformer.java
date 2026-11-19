package datawave.query.transformer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.log4j.Logger;

import datawave.core.query.cachedresults.CacheableLogic;
import datawave.core.query.logic.BaseQueryLogicTransformer;
import datawave.microservice.query.Query;
import datawave.query.Constants;
import datawave.visibility.AccessExpressionUtil;
import datawave.webservice.query.cachedresults.CacheableQueryRow;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.query.result.event.EventBase;
import datawave.webservice.query.result.event.FieldBase;
import datawave.webservice.query.result.event.Metadata;
import datawave.webservice.query.result.event.ResponseObjectFactory;
import datawave.webservice.result.BaseQueryResponse;
import datawave.webservice.result.EventQueryResponseBase;

public class ShardQueryCountTableTransformer extends BaseQueryLogicTransformer<Entry<Long,ColumnVisibility>,EventBase> implements CacheableLogic {
    public static final String COUNT_CELL = "count";

    private Authorizations auths = null;

    private static final Logger log = Logger.getLogger(ShardQueryCountTableTransformer.class);

    private List<String> variableFieldList = null;
    private ResponseObjectFactory responseObjectFactory;

    public ShardQueryCountTableTransformer(Query settings, ResponseObjectFactory responseObjectFactory) {
        super();
        this.responseObjectFactory = responseObjectFactory;
        this.auths = new Authorizations(settings.getQueryAuthorizations().split(","));
    }

    @Override
    public EventBase transform(Entry<Long,ColumnVisibility> untypedEntry) {

        Long count = untypedEntry.getKey();
        ColumnVisibility vis = untypedEntry.getValue();

        EventBase e = this.responseObjectFactory.getEvent();

        String visibility;
        try {
            visibility = AccessExpressionUtil.normalize(vis).getExpression();
        } catch (RuntimeException e1) {
            throw new IllegalArgumentException("Unable to normalize visibility", e1);
        }

        e.setColumnVisibility(visibility);
        FieldBase field = this.makeField(COUNT_CELL, vis, System.currentTimeMillis(), count);

        List<FieldBase> fields = new ArrayList<>();
        fields.add(field);
        e.setFields(fields);

        Metadata metadata = new Metadata();
        metadata.setDataType(Constants.EMPTY_STRING);
        metadata.setInternalId(field.getName()); // There is only one item returned for the entire query logic.
        metadata.setRow(Constants.EMPTY_STRING);
        e.setMetadata(metadata);
        e.setSizeInBytes(fields.size() * 6L);
        return e;
    }

    private FieldBase makeField(String name, ColumnVisibility columnVisibility, Long timestamp, Object value) {
        FieldBase field = this.responseObjectFactory.getField();
        field.setName(name);
        field.setColumnVisibility(AccessExpressionUtil.toAccessExpression(columnVisibility));
        field.setTimestamp(timestamp);
        field.setValue(value);
        return field;
    }

    @Override
    public BaseQueryResponse createResponse(List<Object> resultList) {
        EventQueryResponseBase response = responseObjectFactory.getEventQueryResponse();
        List<EventBase> eventList = new ArrayList<>();
        for (Object o : resultList) {
            EventBase e = (EventBase) o;
            eventList.add(e);
        }
        response.setFields(variableFieldList);
        response.setEvents(eventList);
        response.setReturnedEvents((long) eventList.size());
        return response;
    }

    @Override
    public CacheableQueryRow writeToCache(Object o) throws QueryException {

        EventBase event = (EventBase) o;

        CacheableQueryRow cqo = responseObjectFactory.getCacheableQueryRow();
        Metadata metadata = event.getMetadata();
        cqo.setColFam(metadata.getDataType() + ":" + cqo.getEventId());
        cqo.setDataType(metadata.getDataType());
        cqo.setEventId(metadata.getInternalId());
        cqo.setRow(metadata.getRow());

        List<FieldBase> fields = event.getFields();
        for (FieldBase f : fields) {
            cqo.addColumn(f.getName(), f.getTypedValue(), f.getColumnVisibility(), f.getTimestamp());
        }

        // set the size in bytes using the initial event size as an approximation
        cqo.setSizeInBytes(event.getSizeInBytes());

        return cqo;
    }

    @Override
    public Object readFromCache(CacheableQueryRow cacheableQueryRow) {
        if (this.variableFieldList == null) {
            this.variableFieldList = cacheableQueryRow.getVariableColumnNames();
        }
        String dataType = cacheableQueryRow.getDataType();
        String internalId = cacheableQueryRow.getEventId();
        String row = cacheableQueryRow.getRow();

        EventBase event = responseObjectFactory.getEvent();

        event.setColumnVisibility(cacheableQueryRow.getColumnVisibility());

        Metadata metadata = new Metadata();
        metadata.setDataType(dataType);
        metadata.setInternalId(internalId);
        metadata.setRow(row);
        event.setMetadata(metadata);

        List<FieldBase> fieldList = new ArrayList<>();
        Map<String,String> columnValueMap = cacheableQueryRow.getColumnValues();
        for (Map.Entry<String,String> entry : columnValueMap.entrySet()) {
            String columnName = entry.getKey();
            String columnValue = entry.getValue();
            String columnVisibility = cacheableQueryRow.getColumnVisibility(columnName);
            Long columnTimestamp = cacheableQueryRow.getColumnTimestamp(columnName);
            FieldBase field = responseObjectFactory.getField();
            field.setName(columnName);
            field.setColumnVisibility(columnVisibility);
            field.setTimestamp(columnTimestamp);
            field.setValue(columnValue);
            fieldList.add(field);
        }
        event.setFields(fieldList);
        return event;
    }
}

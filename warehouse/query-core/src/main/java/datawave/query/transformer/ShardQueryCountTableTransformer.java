package datawave.query.transformer;

import datawave.marking.MarkingFunctions;
import datawave.marking.MarkingFunctions.Exception;
import datawave.query.Constants;
import datawave.webservice.query.Query;
import datawave.webservice.query.cachedresults.CacheableLogic;
import datawave.webservice.query.cachedresults.CacheableQueryRow;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.query.logic.BaseQueryLogicTransformer;
import datawave.webservice.query.result.event.EventBase;
import datawave.webservice.query.result.event.FieldBase;
import datawave.webservice.query.result.event.Metadata;
import datawave.webservice.query.result.event.ResponseObjectFactory;
import datawave.webservice.result.BaseQueryResponse;
import datawave.webservice.result.EventQueryResponseBase;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class ShardQueryCountTableTransformer extends BaseQueryLogicTransformer<Entry<Long,ColumnVisibility>,EventBase> implements CacheableLogic {
    public static final String COUNT_CELL = "count";

    private Authorizations auths = null;

    private static final Logger log = Logger.getLogger(ShardQueryCountTableTransformer.class);

    private List<String> variableFieldList = null;
    private ResponseObjectFactory responseObjectFactory;

    public ShardQueryCountTableTransformer(Query settings, MarkingFunctions markingFunctions, ResponseObjectFactory responseObjectFactory) {
        super(markingFunctions);
        this.responseObjectFactory = responseObjectFactory;
        this.auths = new Authorizations(settings.getQueryAuthorizations().split(","));
    }

    @Override
    public EventBase transform(Entry<Long,ColumnVisibility> untypedEntry) {

        Long count = untypedEntry.getKey();
        ColumnVisibility vis = untypedEntry.getValue();

        Map<String,String> markings;
        try {
            markings = markingFunctions.translateFromColumnVisibilityForAuths(vis, auths);
        } catch (Exception e1) {
            throw new IllegalArgumentException("Unable to translate markings", e1);
        }

        EventBase e = this.responseObjectFactory.getEvent();
        e.setMarkings(markings);

        FieldBase field = this.makeField(COUNT_CELL, markings, vis, System.currentTimeMillis(), count);
        e.setMarkings(markings);

        List<FieldBase> fields = new ArrayList<>();
        fields.add(field);
        e.setFields(fields);

        Metadata metadata = new Metadata();
        metadata.setDataType(Constants.EMPTY_STRING);
        metadata.setInternalId(field.getName()); // There is only one item returned for the entire query logic.
        metadata.setRow(Constants.EMPTY_STRING);
        e.setMetadata(metadata);

        return e;
    }

    private FieldBase makeField(String name, Map<String,String> markings, ColumnVisibility columnVisibility, Long timestamp, Object value) {
        FieldBase field = this.responseObjectFactory.getField();
        field.setName(name);
        field.setMarkings(markings);
        field.setColumnVisibility(columnVisibility);
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
    public List<CacheableQueryRow> writeToCache(Object o) throws QueryException {

        List<CacheableQueryRow> cqoList = new ArrayList<>();
        EventBase event = (EventBase) o;

        CacheableQueryRow cqo = responseObjectFactory.getCacheableQueryRow();
        Metadata metadata = event.getMetadata();
        cqo.setColFam(metadata.getDataType() + ":" + cqo.getEventId());
        cqo.setDataType(metadata.getDataType());
        cqo.setEventId(metadata.getInternalId());
        cqo.setRow(metadata.getRow());

        List<FieldBase> fields = event.getFields();
        for (FieldBase f : fields) {
            cqo.addColumn(f.getName(), f.getTypedValue(), f.getMarkings(), f.getColumnVisibility(), f.getTimestamp());
        }

        // set the size in bytes using the initial event size as an approximation
        cqo.setSizeInBytes(event.getSizeInBytes());

        cqoList.add(cqo);
        return cqoList;
    }

    @Override
    public List<Object> readFromCache(List<CacheableQueryRow> cacheableQueryRowList) {

        List<Object> eventList = new ArrayList<>();

        for (CacheableQueryRow cqr : cacheableQueryRowList) {
            if (this.variableFieldList == null) {
                this.variableFieldList = cqr.getVariableColumnNames();
            }
            Map<String,String> markings = cqr.getMarkings();
            String dataType = cqr.getDataType();
            String internalId = cqr.getEventId();
            String row = cqr.getRow();

            EventBase event = responseObjectFactory.getEvent();

            event.setMarkings(markings);

            Metadata metadata = new Metadata();
            metadata.setDataType(dataType);
            metadata.setInternalId(internalId);
            metadata.setRow(row);
            event.setMetadata(metadata);

            List<FieldBase> fieldList = new ArrayList<>();
            Map<String,String> columnValueMap = cqr.getColumnValues();
            for (Map.Entry<String,String> entry : columnValueMap.entrySet()) {
                String columnName = entry.getKey();
                String columnValue = entry.getValue();
                String columnVisibility = cqr.getColumnVisibility(columnName);
                Long columnTimestamp = cqr.getColumnTimestamp(columnName);
                Map<String,String> columnMarkings = cqr.getColumnMarkings(columnName);
                FieldBase field = responseObjectFactory.getField();
                field.setName(columnName);
                field.setMarkings(columnMarkings);
                field.setColumnVisibility(columnVisibility);
                field.setTimestamp(columnTimestamp);
                field.setValue(columnValue);
                fieldList.add(field);
            }
            event.setFields(fieldList);
            eventList.add(event);
        }

        return eventList;
    }
}

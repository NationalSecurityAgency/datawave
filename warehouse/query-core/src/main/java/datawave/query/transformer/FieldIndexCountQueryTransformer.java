package datawave.query.transformer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import datawave.data.hash.UID;
import datawave.marking.MarkingFunctions;
import datawave.query.Constants;
import datawave.query.tables.shard.FieldIndexCountQueryLogic.Tuple;
import datawave.util.TextUtil;
import datawave.webservice.query.Query;
import datawave.webservice.query.cachedresults.CacheableLogic;
import datawave.webservice.query.cachedresults.CacheableQueryRow;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.query.logic.BaseQueryLogic;
import datawave.webservice.query.logic.BaseQueryLogicTransformer;
import datawave.webservice.query.result.event.EventBase;
import datawave.webservice.query.result.event.FieldBase;
import datawave.webservice.query.result.event.ResponseObjectFactory;
import datawave.webservice.query.result.event.Metadata;
import datawave.webservice.result.BaseQueryResponse;
import datawave.webservice.result.EventQueryResponseBase;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

public class FieldIndexCountQueryTransformer extends BaseQueryLogicTransformer<Entry<String,Tuple>,EventBase> implements CacheableLogic {

    private Authorizations auths = null;
    private Logger log = Logger.getLogger(FieldIndexCountQueryTransformer.class);
    private List<String> variableFieldList = null;
    private BaseQueryLogic<Entry<Key,Value>> logic = null;
    private ResponseObjectFactory responseObjectFactory;

    public FieldIndexCountQueryTransformer(BaseQueryLogic<Entry<Key,Value>> logic, Query settings, MarkingFunctions markingFunctions,
                    ResponseObjectFactory responseObjectFactory) {
        super(markingFunctions);
        this.auths = new Authorizations(settings.getQueryAuthorizations().split(","));
        this.logic = logic;
        this.responseObjectFactory = responseObjectFactory;
    }

    @Override
    public EventBase transform(Entry<String,Tuple> entry) {

        if (entry.getKey() == null && entry.getValue() == null) {
            return null;
        }

        if (null == entry.getKey() || null == entry.getValue()) {
            throw new IllegalArgumentException("Null key or value. Key:" + entry.getKey() + ", Value: " + entry.getValue());
        }

        String key = entry.getKey();
        Tuple val = entry.getValue();

        Map<String,String> markings = null;
        try {
            markings = this.markingFunctions.translateFromColumnVisibilityForAuths(val.getColumnVisibility(), this.auths);
        } catch (Exception e) {
            log.error("could not translate " + val.getColumnVisibility() + " to markings, skipping entry");
            return null;
        }
        if (null == markings || markings.isEmpty()) {
            // can't process this one because we did not have valid security markings
            log.error("Transformer visibility interpreter was null, skipping entry");
            return null;
        }

        EventBase event = this.responseObjectFactory.getEvent();

        event.setMarkings(markings);

        List<FieldBase> fields = new ArrayList<>();

        int idx1 = key.indexOf(Constants.NULL_BYTE_STRING);
        if (idx1 == -1) {
            log.error("Invalid entry to FieldIndexCountQueryTransformer");
        }

        // NOTE: fieldName has already been re-mapped by query model in the queryLogic class.
        String fieldName = key.substring(0, idx1);
        String fieldValue;
        String dataType = null;

        // parse from the end to handle nasty field values
        int idx2 = key.lastIndexOf(Constants.NULL_BYTE_STRING);
        if (idx2 > idx1) {
            fieldValue = key.substring(idx1 + 1, idx2);
            dataType = key.substring(idx2 + 1);
        } else {
            fieldValue = key.substring(idx1 + 1);
        }

        String colVis = new String(val.getColumnVisibility().getExpression());

        fields.add(this.makeField("FIELD", markings, colVis, 0L, fieldName));
        fields.add(this.makeField("VALUE", markings, colVis, 0L, fieldValue));

        if (dataType != null) {
            fields.add(this.makeField("DATATYPE", markings, colVis, 0L, dataType));
        }

        fields.add(this.makeField("MOST_RECENT", markings, colVis, 0L, Long.toString(val.getMaxTimestamp())));
        fields.add(this.makeField("RECORD_COUNT", markings, colVis, 0L, Long.toString(val.getCount())));

        event.setFields(fields);
        Text uid = new Text(fieldName);
        TextUtil.textAppend(uid, fieldValue);
        if (null != dataType) {
            TextUtil.textAppend(uid, dataType);
        }

        Metadata metadata = new Metadata();
        metadata.setDataType(Constants.EMPTY_STRING);
        metadata.setInternalId((UID.builder().newId(uid.getBytes())).toString());
        metadata.setRow(Constants.EMPTY_STRING);
        metadata.setTable(logic.getTableName());
        event.setMetadata(metadata);

        if (log.isTraceEnabled()) {
            log.trace("Transformer returning: ");
            log.trace("\tfieldName: " + fieldName);
            log.trace("\tfieldValue: " + fieldValue);
            log.trace("\tdataType: " + ((null != dataType) ? dataType : ""));
            log.trace("\tcount: " + Long.toString(val.getCount()));
            log.trace("\ttimestamp: " + Long.toString(val.getMaxTimestamp()));
        }
        return event;
    }

    @Override
    public BaseQueryResponse createResponse(List<Object> resultList) {
        EventQueryResponseBase response = responseObjectFactory.getEventQueryResponse();
        List<EventBase> eventList = new ArrayList<>();
        for (Object o : resultList) {
            EventBase<?,?> e = (EventBase<?,?>) o;
            eventList.add(e);
        }
        response.setFields(variableFieldList);
        response.setEvents(eventList);
        response.setReturnedEvents((long) eventList.size());
        return (BaseQueryResponse) response;
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

        List<? extends FieldBase> fields = event.getFields();
        for (FieldBase f : fields) {
            cqo.addColumn(f.getName(), f.getTypedValue(), f.getMarkings(), f.getColumnVisibility(), f.getTimestamp());
        }

        // set the size in bytes using the initial event size as an approximation
        cqo.setSizeInBytes(event.getSizeInBytes());

        cqoList.add(cqo);

        return cqoList;
    }

    private FieldBase makeField(String name, Map<String,String> markings, String columnVisibility, Long timestamp, Object value) {
        FieldBase field = this.responseObjectFactory.getField();
        field.setName(name);
        field.setMarkings(markings);
        field.setColumnVisibility(columnVisibility);
        field.setTimestamp(timestamp);
        field.setValue(value);
        return field;
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

            EventBase event = this.responseObjectFactory.getEvent();
            event.setMarkings(markings);

            Metadata metadata = new Metadata();
            metadata.setDataType(dataType);
            metadata.setInternalId(internalId);
            metadata.setRow(row);
            metadata.setTable(logic.getTableName());
            event.setMetadata(metadata);

            List<FieldBase> fieldList = new ArrayList<>();
            Map<String,String> columnValueMap = cqr.getColumnValues();
            for (Map.Entry<String,String> entry : columnValueMap.entrySet()) {
                String columnName = entry.getKey();
                String columnValue = entry.getValue();
                Map<String,String> columnMarkings = cqr.getColumnMarkings(columnName);
                String columnVisibility = cqr.getColumnVisibility(columnName);
                Long columnTimestamp = cqr.getColumnTimestamp(columnName);
                FieldBase field = this.makeField(columnName, columnMarkings, columnVisibility, columnTimestamp, columnValue);
                fieldList.add(field);
            }
            event.setFields(fieldList);
            eventList.add(event);
        }

        return eventList;
    }
}

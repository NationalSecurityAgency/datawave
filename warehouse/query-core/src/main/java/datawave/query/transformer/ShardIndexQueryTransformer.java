package datawave.query.transformer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.log4j.Logger;

import com.google.protobuf.InvalidProtocolBufferException;

import datawave.core.query.cachedresults.CacheableLogic;
import datawave.core.query.logic.BaseQueryLogic;
import datawave.core.query.logic.BaseQueryLogicTransformer;
import datawave.ingest.protobuf.Uid;
import datawave.marking.MarkingFunctions;
import datawave.marking.MarkingFunctions.Exception;
import datawave.microservice.query.Query;
import datawave.query.model.QueryModel;
import datawave.webservice.query.cachedresults.CacheableQueryRow;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.query.result.event.EventBase;
import datawave.webservice.query.result.event.FieldBase;
import datawave.webservice.query.result.event.Metadata;
import datawave.webservice.query.result.event.ResponseObjectFactory;
import datawave.webservice.result.BaseQueryResponse;
import datawave.webservice.result.EventQueryResponseBase;

public class ShardIndexQueryTransformer extends BaseQueryLogicTransformer<Entry<Key,Value>,EventBase> implements CacheableLogic {

    private Authorizations auths = null;
    private Logger log = Logger.getLogger(ShardIndexQueryTransformer.class);
    private List<String> variableFieldList = null;
    private BaseQueryLogic<Entry<Key,Value>> logic = null;
    private QueryModel myQueryModel = null;
    private ResponseObjectFactory responseObjectFactory;

    public ShardIndexQueryTransformer(BaseQueryLogic<Entry<Key,Value>> logic, Query settings, MarkingFunctions markingFunctions,
                    ResponseObjectFactory responseObjectFactory, QueryModel qm) {
        super(markingFunctions);
        this.responseObjectFactory = responseObjectFactory;
        this.auths = new Authorizations(settings.getQueryAuthorizations().split(","));
        this.logic = logic;
        this.myQueryModel = qm;
    }

    @Override
    public EventBase transform(Entry<Key,Value> input) {

        log.debug("Transform got " + input);
        @SuppressWarnings("unchecked")
        Entry<Key,Value> entry = (Entry<Key,Value>) input;

        if (entry.getKey() == null && entry.getValue() == null) {
            return null;
        }

        if (null == entry.getKey() || null == entry.getValue()) {
            throw new IllegalArgumentException("Null key or value. Key:" + entry.getKey() + ", Value: " + entry.getValue());
        }

        EventBase event = responseObjectFactory.getEvent();
        ColumnVisibility columnVisibility = new ColumnVisibility(entry.getKey().getColumnVisibility());
        Map<String,String> markings;
        try {
            markings = this.markingFunctions.translateFromColumnVisibilityForAuths(columnVisibility, this.auths);
        } catch (Exception e1) {
            throw new RuntimeException("could not make markings from: " + columnVisibility);
        }
        event.setMarkings(markings);

        List<FieldBase> fields = new ArrayList<>();

        Key key = entry.getKey();
        String row = key.getRow().toString();
        String cf = key.getColumnFamily().toString();
        String cq = key.getColumnQualifier().toString();
        String cv = key.getColumnVisibility().toString();

        fields.add(makeField("VALUE", markings, cv, 0L, row));
        /**
         * Added query model to alias FIELD
         */
        fields.add(makeField("FIELD", markings, cv, 0L, myQueryModel.aliasFieldNameReverseModel(cf)));
        fields.add(makeField("DATE", markings, cv, 0L, cq.substring(0, 8)));
        fields.add(makeField("DATA TYPE", markings, cv, 0L, cq.substring(9)));
        // Parse the UID.List object from the value
        Uid.List uidList = null;
        long count = 0;
        try {
            uidList = Uid.List.parseFrom(entry.getValue().get());
            if (null != uidList) {
                count = uidList.getCOUNT();
            }
        } catch (InvalidProtocolBufferException e) {
            log.error("Failed to parse Uid List", e);
        }
        fields.add(makeField("RECORD COUNT", markings, cv, 0L, Long.toString(count)));
        event.setFields(fields);

        Metadata metadata = new Metadata();
        String id = logic.getTableName() + ":" + row + ":" + cf + ":" + cq;
        metadata.setInternalId(UUID.nameUUIDFromBytes(id.getBytes()).toString());
        metadata.setDataType(entry.getKey().getColumnFamily().toString());
        metadata.setRow(entry.getKey().getRow().toString());
        metadata.setTable(logic.getTableName());
        event.setMetadata(metadata);
        return event;
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
    public BaseQueryResponse createResponse(List<Object> resultList) {
        EventQueryResponseBase response = responseObjectFactory.getEventQueryResponse();
        List<EventBase> eventList = new ArrayList<>();
        for (Object o : resultList) {
            EventBase e = (EventBase) o;
            eventList.add(e);

        }
        response.setFields(this.variableFieldList);
        response.setEvents(eventList);
        response.setReturnedEvents((long) eventList.size());
        return response;
    }

    @Override
    public CacheableQueryRow writeToCache(Object o) throws QueryException {
        EventBase event = (EventBase) o;

        CacheableQueryRow cqo = responseObjectFactory.getCacheableQueryRow();
        cqo.setMarkingFunctions(this.markingFunctions);
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

        return cqo;
    }

    @Override
    public Object readFromCache(CacheableQueryRow cacheableQueryRow) {
        if (this.variableFieldList == null) {
            this.variableFieldList = cacheableQueryRow.getVariableColumnNames();
        }
        Map<String,String> markings = cacheableQueryRow.getMarkings();
        String dataType = cacheableQueryRow.getDataType();
        String internalId = cacheableQueryRow.getEventId();
        String row = cacheableQueryRow.getRow();

        EventBase event = responseObjectFactory.getEvent();

        event.setMarkings(markings);

        Metadata metadata = new Metadata();
        metadata.setDataType(dataType);
        metadata.setInternalId(internalId);
        metadata.setRow(row);
        metadata.setTable(logic.getTableName());
        event.setMetadata(metadata);

        List<FieldBase> fieldList = new ArrayList<>();
        Map<String,String> columnValueMap = cacheableQueryRow.getColumnValues();
        for (Map.Entry<String,String> entry : columnValueMap.entrySet()) {
            String columnName = entry.getKey();
            String columnValue = entry.getValue();
            String columnVisibility = cacheableQueryRow.getColumnVisibility(columnName);
            Long columnTimestamp = cacheableQueryRow.getColumnTimestamp(columnName);
            Map<String,String> columnMarkings = cacheableQueryRow.getColumnMarkings(columnName);
            FieldBase field = responseObjectFactory.getField();
            field.setName(columnName);
            field.setMarkings(columnMarkings);
            field.setColumnVisibility(columnVisibility);
            field.setTimestamp(columnTimestamp);
            field.setValue(columnValue);
            fieldList.add(field);
        }
        event.setFields(fieldList);

        return event;
    }
}

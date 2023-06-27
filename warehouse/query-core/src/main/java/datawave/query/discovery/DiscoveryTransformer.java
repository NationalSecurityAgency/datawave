package datawave.query.discovery;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Writable;

import com.google.common.base.Preconditions;

import datawave.core.query.cachedresults.CacheableLogic;
import datawave.core.query.logic.BaseQueryLogic;
import datawave.core.query.logic.BaseQueryLogicTransformer;
import datawave.marking.MarkingFunctions;
import datawave.marking.MarkingFunctions.Exception;
import datawave.query.model.QueryModel;
import datawave.webservice.query.Query;
import datawave.webservice.query.cachedresults.CacheableQueryRow;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.query.result.event.EventBase;
import datawave.webservice.query.result.event.FieldBase;
import datawave.webservice.query.result.event.Metadata;
import datawave.webservice.query.result.event.ResponseObjectFactory;
import datawave.webservice.result.BaseQueryResponse;
import datawave.webservice.result.EventQueryResponseBase;

public class DiscoveryTransformer extends BaseQueryLogicTransformer<DiscoveredThing,EventBase> implements CacheableLogic {
    private List<String> variableFieldList = null;
    private BaseQueryLogic<DiscoveredThing> logic = null;
    private QueryModel myQueryModel = null;
    private MarkingFunctions markingFunctions;
    private ResponseObjectFactory responseObjectFactory;

    public DiscoveryTransformer(BaseQueryLogic<DiscoveredThing> logic, Query settings, QueryModel qm) {
        super(new MarkingFunctions.Default());
        this.markingFunctions = logic.getMarkingFunctions();
        this.responseObjectFactory = logic.getResponseObjectFactory();
        this.logic = logic;
        this.myQueryModel = qm;
    }

    @Override
    public EventBase transform(DiscoveredThing thing) {
        Preconditions.checkNotNull(thing, "Received a null object to transform!");

        EventBase event = this.responseObjectFactory.getEvent();
        Map<String,String> markings;
        try {
            markings = this.markingFunctions.translateFromColumnVisibility(new ColumnVisibility(thing.getColumnVisibility()));
        } catch (Exception e) {
            throw new RuntimeException("could not parse to markings: " + thing.getColumnVisibility());
        }
        event.setMarkings(markings);

        List<FieldBase> fields = new ArrayList<>();

        fields.add(this.makeField("VALUE", markings, "", 0L, thing.getTerm()));
        /**
         * Added query model to alias FIELD
         */
        fields.add(this.makeField("FIELD", markings, "", 0L, myQueryModel.aliasFieldNameReverseModel(thing.getField())));
        fields.add(this.makeField("DATE", markings, "", 0L, thing.getDate()));
        fields.add(this.makeField("DATA TYPE", markings, "", 0L, thing.getType()));

        // If requested return counts separated by colvis, all counts by colvis could be > total record count
        if (thing.getCountsByColumnVisibility() != null && !thing.getCountsByColumnVisibility().isEmpty()) {
            for (Map.Entry<Writable,Writable> entry : thing.getCountsByColumnVisibility().entrySet()) {
                try {
                    Map<String,String> eMarkings = this.markingFunctions.translateFromColumnVisibility(new ColumnVisibility(entry.getKey().toString()));
                    fields.add(this.makeField("RECORD COUNT", new HashMap<>(), entry.getKey().toString(), 0L, entry.getValue().toString()));
                } catch (Exception e) {
                    throw new RuntimeException("could not parse to markings: " + thing.getColumnVisibility());
                }

            }
        } else {
            fields.add(this.makeField("RECORD COUNT", markings, "", 0L, Long.toString(thing.getCount())));
        }

        event.setFields(fields);

        Metadata metadata = new Metadata();
        metadata.setInternalId(""); // there is no UUID for a single index pointer
        metadata.setDataType(thing.getType()); // duplicate
        metadata.setRow(thing.getTerm()); // duplicate
        metadata.setTable(logic.getTableName());
        event.setMetadata(metadata);

        return event;
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

    @Override
    public BaseQueryResponse createResponse(List<Object> resultList) {
        EventQueryResponseBase response = this.responseObjectFactory.getEventQueryResponse();
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

        EventBase event = this.responseObjectFactory.getEvent();

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
            FieldBase field = this.responseObjectFactory.getField();
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

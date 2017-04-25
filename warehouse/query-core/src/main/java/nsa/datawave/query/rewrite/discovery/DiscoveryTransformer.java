package nsa.datawave.query.rewrite.discovery;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import java.util.HashMap;
import nsa.datawave.marking.MarkingFunctions;
import nsa.datawave.marking.MarkingFunctions.Exception;
import nsa.datawave.query.model.QueryModel;
import nsa.datawave.webservice.query.Query;
import nsa.datawave.webservice.query.cachedresults.CacheableLogic;
import nsa.datawave.webservice.query.cachedresults.CacheableQueryRow;
import nsa.datawave.webservice.query.cachedresults.CacheableQueryRowImpl;
import nsa.datawave.webservice.query.exception.QueryException;
import nsa.datawave.webservice.query.logic.BaseQueryLogic;
import nsa.datawave.webservice.query.logic.BaseQueryLogicTransformer;
import nsa.datawave.webservice.query.result.event.EventBase;
import nsa.datawave.webservice.query.result.event.FieldBase;
import nsa.datawave.webservice.query.result.event.ResponseObjectFactory;
import nsa.datawave.webservice.query.result.event.Metadata;
import nsa.datawave.webservice.result.BaseQueryResponse;
import nsa.datawave.webservice.result.EventQueryResponseBase;

import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Writable;

import com.google.common.base.Preconditions;

public class DiscoveryTransformer extends BaseQueryLogicTransformer implements CacheableLogic {
    private List<String> variableFieldList = null;
    private BaseQueryLogic<DiscoveredThing> logic = null;
    private QueryModel myQueryModel = null;
    private MarkingFunctions markingFunctions;
    private ResponseObjectFactory responseObjectFactory;
    
    public DiscoveryTransformer(BaseQueryLogic<DiscoveredThing> logic, Query settings, QueryModel qm) {
        super(new MarkingFunctions.NoOp());
        this.markingFunctions = logic.getMarkingFunctions();
        this.responseObjectFactory = logic.getResponseObjectFactory();
        this.logic = logic;
        this.myQueryModel = qm;
    }
    
    @Override
    public Object transform(Object input) {
        Preconditions.checkNotNull(input, "Received a null object to transform!");
        if (input instanceof DiscoveredThing) {
            DiscoveredThing thing = (DiscoveredThing) input;
            
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
            if (thing.getCountsByColumnVisibility() != null && thing.getCountsByColumnVisibility().size() > 0) {
                for (Map.Entry<Writable,Writable> entry : thing.getCountsByColumnVisibility().entrySet()) {
                    try {
                        Map<String,String> eMarkings = this.markingFunctions.translateFromColumnVisibility(new ColumnVisibility(entry.getKey().toString()));
                        fields.add(this.makeField("RECORD COUNT", new HashMap<String,String>(), entry.getKey().toString(), 0L, entry.getValue().toString()));
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
        } else {
            throw new IllegalArgumentException("Invalid input type: " + input.getClass());
        }
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
    public List<CacheableQueryRow> writeToCache(Object o) throws QueryException {
        
        List<CacheableQueryRow> cqoList = new ArrayList<>();
        EventBase event = (EventBase) o;
        
        CacheableQueryRow cqo = new CacheableQueryRowImpl();
        Metadata metadata = event.getMetadata();
        cqo.setColFam(metadata.getDataType() + ":" + cqo.getEventId());
        cqo.setDataType(metadata.getDataType());
        cqo.setEventId(metadata.getInternalId());
        cqo.setRow(metadata.getRow());
        
        List<FieldBase> fields = event.getFields();
        for (FieldBase f : fields) {
            cqo.addColumn(f.getName(), f.getTypedValue(), f.getMarkings(), f.getColumnVisibility(), f.getTimestamp());
        }
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
                String columnVisibility = cqr.getColumnVisibility(columnName);
                Long columnTimestamp = cqr.getColumnTimestamp(columnName);
                Map<String,String> columnMarkings = cqr.getColumnMarkings(columnName);
                FieldBase field = this.responseObjectFactory.getField();
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

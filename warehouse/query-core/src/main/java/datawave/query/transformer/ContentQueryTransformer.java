package datawave.query.transformer;

import datawave.marking.MarkingFunctions;
import datawave.marking.MarkingFunctions.Exception;
import datawave.query.table.parser.ContentKeyValueFactory;
import datawave.query.table.parser.ContentKeyValueFactory.ContentKeyValue;
import datawave.webservice.query.Query;
import datawave.webservice.query.logic.BaseQueryLogicTransformer;
import datawave.webservice.query.result.event.EventBase;
import datawave.webservice.query.result.event.FieldBase;
import datawave.webservice.query.result.event.Metadata;
import datawave.webservice.query.result.event.ResponseObjectFactory;
import datawave.webservice.result.BaseQueryResponse;
import datawave.webservice.result.EventQueryResponseBase;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

public class ContentQueryTransformer extends BaseQueryLogicTransformer<Entry<Key,Value>,EventBase> {
    
    protected final Authorizations auths;
    protected final ResponseObjectFactory responseObjectFactory;
    
    public ContentQueryTransformer(Query query, MarkingFunctions markingFunctions, ResponseObjectFactory responseObjectFactory) {
        super(markingFunctions);
        this.auths = new Authorizations(query.getQueryAuthorizations().split(","));
        this.responseObjectFactory = responseObjectFactory;
    }
    
    @Override
    public EventBase transform(Entry<Key,Value> entry) {
        
        if (entry.getKey() == null && entry.getValue() == null)
            return null;
        
        if (null == entry.getKey() || null == entry.getValue()) {
            throw new IllegalArgumentException("Null key or value. Key:" + entry.getKey() + ", Value: " + entry.getValue());
        }
        
        ContentKeyValue ckv;
        try {
            ckv = ContentKeyValueFactory.parse(entry.getKey(), entry.getValue(), auths, markingFunctions);
        } catch (Exception e1) {
            throw new IllegalArgumentException("Unable to parse visibility", e1);
        }
        
        EventBase e = responseObjectFactory.getEvent();
        FieldBase field = responseObjectFactory.getField();
        
        e.setMarkings(ckv.getMarkings());
        
        Metadata m = new Metadata();
        m.setRow(ckv.getShardId());
        m.setDataType(ckv.getDatatype());
        m.setInternalId(ckv.getUid());
        e.setMetadata(m);
        
        field.setMarkings(ckv.getMarkings());
        field.setName(ckv.getViewName());
        field.setTimestamp(entry.getKey().getTimestamp());
        field.setValue(ckv.getContents());
        
        List<FieldBase> fields = new ArrayList<>();
        fields.add(field);
        e.setFields(fields);
        
        return e;
    }
    
    @Override
    public BaseQueryResponse createResponse(List<Object> resultList) {
        EventQueryResponseBase response = responseObjectFactory.getEventQueryResponse();
        List<EventBase> eventList = new ArrayList<>();
        for (Object o : resultList) {
            EventBase result = (EventBase) o;
            eventList.add(result);
        }
        response.setEvents(eventList);
        response.setReturnedEvents((long) eventList.size());
        return response;
    }
}

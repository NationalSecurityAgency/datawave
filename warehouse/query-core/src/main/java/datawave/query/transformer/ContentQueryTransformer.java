package datawave.query.transformer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import datawave.marking.MarkingFunctions;
import datawave.marking.MarkingFunctions.Exception;
import datawave.query.table.parser.ContentKeyValueFactory;
import datawave.query.table.parser.ContentKeyValueFactory.ContentKeyValue;
import datawave.webservice.query.Query;
import datawave.webservice.query.logic.BaseQueryLogicTransformer;
import datawave.webservice.query.result.event.DefaultEvent;
import datawave.webservice.query.result.event.DefaultField;
import datawave.webservice.query.result.event.EventBase;
import datawave.webservice.query.result.event.Metadata;
import datawave.webservice.result.BaseQueryResponse;
import datawave.webservice.result.DefaultEventQueryResponse;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.log4j.Logger;

public class ContentQueryTransformer extends BaseQueryLogicTransformer<Entry<Key,Value>,DefaultEvent> {
    
    private Authorizations auths = null;
    private Logger log = Logger.getLogger(ContentQueryTransformer.class);
    
    public ContentQueryTransformer(Query query, MarkingFunctions markingFunctions) {
        super(markingFunctions);
        this.auths = new Authorizations(query.getQueryAuthorizations().split(","));
    }
    
    @Override
    public DefaultEvent transform(Entry<Key,Value> entry) {
        
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
        
        DefaultEvent e = new DefaultEvent();
        DefaultField field = new DefaultField();
        
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
        
        List<DefaultField> fields = new ArrayList<DefaultField>();
        fields.add(field);
        e.setFields(fields);
        
        return e;
        
    }
    
    @Override
    public BaseQueryResponse createResponse(List<Object> resultList) {
        
        DefaultEventQueryResponse response = new DefaultEventQueryResponse();
        List<EventBase> eventList = new ArrayList<>();
        for (Object o : resultList) {
            DefaultEvent result = (DefaultEvent) o;
            eventList.add(result);
        }
        response.setEvents(eventList);
        response.setReturnedEvents((long) eventList.size());
        return response;
    }
}

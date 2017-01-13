package nsa.datawave.query.transformer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import nsa.datawave.marking.MarkingFunctions;
import nsa.datawave.marking.MarkingFunctions.Exception;
import nsa.datawave.query.table.parser.ContentKeyValueFactory;
import nsa.datawave.query.table.parser.ContentKeyValueFactory.ContentKeyValue;
import nsa.datawave.webservice.query.Query;
import nsa.datawave.webservice.query.logic.BaseQueryLogicTransformer;
import nsa.datawave.webservice.query.result.event.DefaultEvent;
import nsa.datawave.webservice.query.result.event.DefaultField;
import nsa.datawave.webservice.query.result.event.EventBase;
import nsa.datawave.webservice.query.result.event.Metadata;
import nsa.datawave.webservice.result.BaseQueryResponse;
import nsa.datawave.webservice.result.DefaultEventQueryResponse;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.log4j.Logger;

public class ContentQueryTransformer extends BaseQueryLogicTransformer {
    
    private Authorizations auths = null;
    private Logger log = Logger.getLogger(ContentQueryTransformer.class);
    
    public ContentQueryTransformer(Query query, MarkingFunctions markingFunctions) {
        super(markingFunctions);
        this.auths = new Authorizations(query.getQueryAuthorizations().split(","));
    }
    
    @Override
    public Object transform(Object input) {
        if (input instanceof Entry<?,?>) {
            @SuppressWarnings("unchecked")
            Entry<Key,org.apache.accumulo.core.data.Value> entry = (Entry<Key,org.apache.accumulo.core.data.Value>) input;
            
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
            
        } else {
            throw new IllegalArgumentException("Invalid input type: " + input.getClass());
        }
        
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

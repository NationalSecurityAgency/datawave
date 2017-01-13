package nsa.datawave.query.rewrite.transformer;

import java.util.Map.Entry;

import nsa.datawave.marking.MarkingFunctions;
import nsa.datawave.query.rewrite.tld.TLD;
import nsa.datawave.webservice.query.Query;
import nsa.datawave.webservice.query.logic.BaseQueryLogic;
import nsa.datawave.webservice.query.result.event.EventBase;
import nsa.datawave.webservice.query.result.event.ResponseObjectFactory;
import nsa.datawave.webservice.query.result.event.Metadata;

import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

public class ParentDocumentTransformer extends DocumentTransformer {
    
    public ParentDocumentTransformer(BaseQueryLogic<Entry<Key,Value>> logic, Query settings, MarkingFunctions markingFunctions,
                    ResponseObjectFactory responseObjectFactory) {
        super(logic, settings, markingFunctions, responseObjectFactory);
    }
    
    public ParentDocumentTransformer(BaseQueryLogic<Entry<Key,Value>> logic, Query settings, MarkingFunctions markingFunctions,
                    ResponseObjectFactory responseObjectFactory, Boolean cellLevelVisibility) {
        super(logic, settings, markingFunctions, responseObjectFactory, cellLevelVisibility);
    }
    
    @Override
    public Object transform(Object input) {
        EventBase event = (EventBase) super.transform(input);
        Metadata md = event.getMetadata();
        byte[] id = md.getInternalId().getBytes();
        ByteSequence parentIdBytes = TLD.parseParentPointerFromId(new ArrayByteSequence(id));
        String parentId = new String(parentIdBytes.getBackingArray(), parentIdBytes.offset(), parentIdBytes.length());
        md.setInternalId(parentId);
        return event;
    }
}

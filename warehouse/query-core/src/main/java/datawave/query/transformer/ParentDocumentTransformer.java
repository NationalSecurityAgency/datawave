package datawave.query.transformer;

import java.util.Map.Entry;

import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

import datawave.marking.MarkingFunctions;
import datawave.query.tld.TLD;
import datawave.webservice.query.Query;
import datawave.webservice.query.logic.BaseQueryLogic;
import datawave.webservice.query.result.event.EventBase;
import datawave.webservice.query.result.event.Metadata;
import datawave.webservice.query.result.event.ResponseObjectFactory;

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
    public EventBase transform(Entry<Key,Value> input) {
        EventBase event = super.transform(input);
        Metadata md = event.getMetadata();
        byte[] id = md.getInternalId().getBytes();
        ByteSequence parentIdBytes = TLD.parseParentPointerFromId(new ArrayByteSequence(id));
        String parentId = new String(parentIdBytes.getBackingArray(), parentIdBytes.offset(), parentIdBytes.length());
        md.setInternalId(parentId);
        return event;
    }
}

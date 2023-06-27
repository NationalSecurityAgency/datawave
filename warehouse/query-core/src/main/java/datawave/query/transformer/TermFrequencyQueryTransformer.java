package datawave.query.transformer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.shaded.com.google.common.collect.ImmutableList;

import datawave.marking.MarkingFunctions;
import datawave.query.table.parser.TermFrequencyKeyValueFactory;
import datawave.query.table.parser.TermFrequencyKeyValueFactory.TermFrequencyKeyValue;
import datawave.webservice.query.Query;
import datawave.webservice.query.exception.EmptyObjectException;
import datawave.webservice.query.logic.BaseQueryLogicTransformer;
import datawave.webservice.query.result.event.EventBase;
import datawave.webservice.query.result.event.FieldBase;
import datawave.webservice.query.result.event.Metadata;
import datawave.webservice.query.result.event.ResponseObjectFactory;
import datawave.webservice.result.BaseQueryResponse;
import datawave.webservice.result.EventQueryResponseBase;

@SuppressWarnings("rawtypes")
public class TermFrequencyQueryTransformer extends BaseQueryLogicTransformer<Entry<Key,Value>,EventBase> {

    private final Query query;
    private final Authorizations auths;
    private final ResponseObjectFactory responseObjectFactory;

    public TermFrequencyQueryTransformer(Query query, MarkingFunctions markingFunctions, ResponseObjectFactory responseObjectFactory) {
        super(markingFunctions);
        this.query = query;
        this.responseObjectFactory = responseObjectFactory;
        this.auths = new Authorizations(StringUtils.split(this.query.getQueryAuthorizations(), ','));
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

    @Override
    public EventBase transform(Entry<Key,Value> entry) throws EmptyObjectException {
        if (entry.getKey() == null && entry.getValue() == null) {
            return null;
        }

        if (entry.getKey() == null || entry.getValue() == null) {
            throw new IllegalArgumentException("Null keyy or value. Key:" + entry.getKey() + ", Value: " + entry.getValue());
        }

        TermFrequencyKeyValue tfkv;
        try {
            tfkv = TermFrequencyKeyValueFactory.parse(entry.getKey(), entry.getValue(), auths, markingFunctions);
        } catch (Exception e) {
            throw new IllegalArgumentException("Unable to parse visibility", e);
        }
        EventBase e = responseObjectFactory.getEvent();

        e.setMarkings(tfkv.getMarkings());

        Metadata m = new Metadata();
        m.setRow(tfkv.getShardId());
        m.setDataType(tfkv.getDatatype());
        m.setInternalId(tfkv.getUid());
        e.setMetadata(m);

        List<FieldBase> fields = ImmutableList.of(createField(tfkv, entry, "FIELD_NAME", tfkv.getFieldName()),
                        createField(tfkv, entry, "FIELD_VALUE", tfkv.getFieldValue()),
                        createField(tfkv, entry, "OFFSET_COUNT", String.valueOf(tfkv.getCount())),
                        createField(tfkv, entry, "OFFSETS", tfkv.getOffsets().toString()));
        e.setFields(fields);

        return e;
    }

    protected FieldBase createField(TermFrequencyKeyValue tfkv, Entry<Key,Value> e, String name, String value) {
        FieldBase field = responseObjectFactory.getField();
        field.setMarkings(tfkv.getMarkings());
        field.setName(name);
        field.setValue(value);
        field.setTimestamp(e.getKey().getTimestamp());
        return field;
    }
}

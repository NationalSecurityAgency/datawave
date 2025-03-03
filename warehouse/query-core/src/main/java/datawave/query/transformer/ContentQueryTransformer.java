package datawave.query.transformer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.log4j.Logger;

import datawave.core.query.logic.BaseQueryLogicTransformer;
import datawave.marking.MarkingFunctions;
import datawave.marking.MarkingFunctions.Exception;
import datawave.microservice.query.Query;
import datawave.query.table.parser.ContentKeyValueFactory;
import datawave.query.table.parser.ContentKeyValueFactory.ContentKeyValue;
import datawave.webservice.query.result.event.EventBase;
import datawave.webservice.query.result.event.FieldBase;
import datawave.webservice.query.result.event.Metadata;
import datawave.webservice.query.result.event.ResponseObjectFactory;
import datawave.webservice.result.BaseQueryResponse;
import datawave.webservice.result.EventQueryResponseBase;

@SuppressWarnings("rawtypes")
public class ContentQueryTransformer extends BaseQueryLogicTransformer<Entry<Key,Value>,EventBase> {

    private static final Logger log = Logger.getLogger(ContentQueryTransformer.class);

    protected final Authorizations auths;
    protected final ResponseObjectFactory responseObjectFactory;
    protected final Map<Metadata,String> metadataIdMap;
    protected final boolean decodeView;

    public ContentQueryTransformer(Query query, MarkingFunctions markingFunctions, ResponseObjectFactory responseObjectFactory) {
        this(query, markingFunctions, responseObjectFactory, false);
    }

    public ContentQueryTransformer(Query query, MarkingFunctions markingFunctions, ResponseObjectFactory responseObjectFactory, boolean decodeView) {
        super(markingFunctions);
        this.auths = new Authorizations(query.getQueryAuthorizations().split(","));
        this.responseObjectFactory = responseObjectFactory;
        this.metadataIdMap = extractMetadadaIdMap(query);
        this.decodeView = decodeView;
    }

    /**
     * Extract optional identifiers from the query. Expected query format is:
     * <p>
     * 'DOCUMENT:shard/datatype/uid!optionalIdentifier1 DOCUMENT:shard/datatype/uid!optionalIdentifier2 .. DOCUMENT:shard/datatype/uid!optionalIdentifier3'
     * <p>
     * The identifiers are not required, so this will parse 'DOCUMENT:shard/datatype/uid' as well.
     *
     * @param querySettings
     *            the current query for which we are transforming results.
     * @return a map of shard/datatye/uid mapped to their corresponding identifiers.
     */
    public Map<Metadata,String> extractMetadadaIdMap(Query querySettings) {
        final String query = querySettings.getQuery().trim();
        final Map<Metadata,String> metadataIdMap = new HashMap<>();

        int termIndex = 0;
        while (termIndex < query.length()) {

            // find individual terms.
            int termSeparation = query.indexOf(' ', termIndex);
            final String term;
            if (termSeparation >= 0) {
                term = query.substring(termIndex, termSeparation);
                termIndex = termSeparation + 1;
            } else {
                term = query.substring(termIndex);
                termIndex = query.length();
            }

            // ignore empty terms.
            if (!term.isEmpty()) {
                // trim off the field if there is one.
                int fieldSeparation = term.indexOf(':');
                final String valueIdentifier = fieldSeparation > 0 ? term.substring(fieldSeparation + 1) : term;

                // find the identifier if there is one, otherwise we're done with this term.
                int idSeparation = valueIdentifier.indexOf("!");
                if (idSeparation > 0) {
                    String value = valueIdentifier.substring(0, idSeparation);
                    String identifier = valueIdentifier.substring(idSeparation + 1);

                    final String[] parts = value.split("/");
                    if (parts.length != 3) {
                        throw new IllegalArgumentException("Query term does not specify all needed parts: " + query
                                        + ". Each space-delimited term should be of the form 'DOCUMENT:shardId/datatype/eventUID!optionalIdentifier'.");
                    }

                    final String shardId = parts[0];
                    final String datatype = parts[1];
                    final String uid = parts[2];

                    Metadata md = new Metadata();
                    md.setRow(shardId);
                    md.setDataType(datatype);
                    md.setInternalId(uid);
                    metadataIdMap.put(md, identifier);

                    log.debug("Added identifier " + identifier + " for pieces: " + shardId + ", " + datatype + ", " + uid);
                }
            }
        }

        return metadataIdMap;
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

        // capture the metadata that identifies the field.
        Metadata m = new Metadata();
        m.setRow(ckv.getShardId());
        m.setDataType(ckv.getDatatype());
        m.setInternalId(ckv.getUid());
        e.setMetadata(m);

        // store the content in a field based on it's view name.
        field.setMarkings(ckv.getMarkings());
        field.setName(ckv.getViewName());
        field.setTimestamp(entry.getKey().getTimestamp());
        if (this.decodeView) {
            // settings a String value causes the value not to be base64 encoded, see TypedValue
            field.setValue(new String(ckv.getContents()));
        } else {
            // settings a byte value causes the value to be base64 encoded, see TypedValue
            field.setValue(ckv.getContents());
        }

        List<FieldBase> fields = new ArrayList<>();
        fields.add(field);

        // if an identifier is present for this event, enrich the event with the identifier by adding it as a field.
        String identifier = metadataIdMap.get(m);
        if (identifier != null) {
            FieldBase idField = responseObjectFactory.getField();
            idField.setMarkings(ckv.getMarkings());
            idField.setName("IDENTIFIER");
            idField.setTimestamp(entry.getKey().getTimestamp());
            idField.setValue(identifier);
            fields.add(idField);
        }

        e.setSizeInBytes(fields.size() * 6);
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

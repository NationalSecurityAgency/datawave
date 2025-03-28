package datawave.query.tables.ssdeep;

import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.core.security.Authorizations;

import datawave.core.query.exception.EmptyObjectException;
import datawave.core.query.logic.BaseQueryLogicTransformer;
import datawave.marking.MarkingFunctions;
import datawave.microservice.query.Query;
import datawave.query.config.SSDeepSimilarityQueryConfiguration;
import datawave.webservice.query.result.event.EventBase;
import datawave.webservice.query.result.event.FieldBase;
import datawave.webservice.query.result.event.ResponseObjectFactory;
import datawave.webservice.result.BaseQueryResponse;
import datawave.webservice.result.EventQueryResponseBase;

/**
 * Transforms the results from an SSDeepSimilarityQuery into an EventBase suitable for a datawave web service api response
 */
public class SSDeepSimilarityQueryTransformer extends BaseQueryLogicTransformer<ScoredSSDeepPair,EventBase> {

    protected final Authorizations auths;

    protected final ResponseObjectFactory responseObjectFactory;

    public SSDeepSimilarityQueryTransformer(Query query, SSDeepSimilarityQueryConfiguration config, MarkingFunctions markingFunctions,
                    ResponseObjectFactory responseObjectFactory) {
        super(markingFunctions);
        this.auths = new Authorizations(query.getQueryAuthorizations().split(","));
        this.responseObjectFactory = responseObjectFactory;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public EventBase transform(ScoredSSDeepPair pair) throws EmptyObjectException {
        final EventBase event = responseObjectFactory.getEvent();
        final List<FieldBase> fields = new ArrayList<>();

        {
            FieldBase field = responseObjectFactory.getField();
            field.setName("MATCHING_SSDEEP");
            field.setValue(pair.getMatchingHash().toString());
            fields.add(field);
        }

        {
            FieldBase field = responseObjectFactory.getField();
            field.setName("QUERY_SSDEEP");
            field.setValue(pair.getQueryHash().toString());
            fields.add(field);
        }

        {
            FieldBase field = responseObjectFactory.getField();
            field.setName("WEIGHTED_SCORE");
            field.setValue(String.valueOf(pair.getWeightedScore()));
            fields.add(field);
        }

        {
            FieldBase field = responseObjectFactory.getField();
            field.setName("OVERLAP_SCORE");
            field.setValue(String.valueOf(pair.getOverlapScore()));
            fields.add(field);
        }

        {
            FieldBase field = responseObjectFactory.getField();
            field.setName("OVERLAP_SSDEEP_NGRAMS");
            field.setValue(pair.getOverlapsAsString());
            fields.add(field);
        }

        event.setFields(fields);

        return event;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public BaseQueryResponse createResponse(List<Object> resultList) {
        final EventQueryResponseBase eventResponse = responseObjectFactory.getEventQueryResponse();
        final List<EventBase> events = new ArrayList<>();

        for (Object o : resultList) {
            EventBase event = (EventBase) o;
            events.add(event);
        }

        eventResponse.setEvents(events);

        return eventResponse;
    }
}

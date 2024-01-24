package datawave.query.tables.ssdeep;

import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.core.security.Authorizations;

import datawave.marking.MarkingFunctions;
import datawave.query.config.SSDeepSimilarityQueryConfiguration;
import datawave.webservice.query.Query;
import datawave.webservice.query.exception.EmptyObjectException;
import datawave.webservice.query.logic.BaseQueryLogicTransformer;
import datawave.webservice.query.result.event.EventBase;
import datawave.webservice.query.result.event.FieldBase;
import datawave.webservice.query.result.event.ResponseObjectFactory;
import datawave.webservice.result.BaseQueryResponse;
import datawave.webservice.result.EventQueryResponseBase;

public class SSDeepSimilarityQueryTransformer extends BaseQueryLogicTransformer<ScoredSSDeepPair,EventBase> {

    public static final String MIN_SSDEEP_SCORE_PARAMETER = "minScore";

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

        FieldBase f = responseObjectFactory.getField();
        f.setName("MATCHING_SSDEEP");
        f.setValue(pair.getMatchingHash().toString());
        fields.add(f);

        f = responseObjectFactory.getField();
        f.setName("QUERY_SSDEEP");
        f.setValue(pair.getQueryHash().toString());
        fields.add(f);

        f = responseObjectFactory.getField();
        f.setName("MATCH_SCORE");
        f.setValue(String.valueOf(pair.getOverlapScore()));
        fields.add(f);

        f = responseObjectFactory.getField();
        f.setName("WEIGHTED_SCORE");
        f.setValue(String.valueOf(pair.getWeightedScore()));
        fields.add(f);

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

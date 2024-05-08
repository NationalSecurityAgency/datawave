package datawave.query.tables;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.log4j.Logger;

import datawave.marking.MarkingFunctions;
import datawave.query.tables.remote.RemoteQueryLogic;
import datawave.query.transformer.EventQueryTransformerSupport;
import datawave.webservice.common.logging.ThreadConfigurableLogger;
import datawave.webservice.query.Query;
import datawave.webservice.query.configuration.GenericQueryConfiguration;
import datawave.webservice.query.exception.EmptyObjectException;
import datawave.webservice.query.logic.BaseQueryLogic;
import datawave.webservice.query.logic.QueryLogicTransformer;
import datawave.webservice.query.result.event.EventBase;
import datawave.webservice.query.result.event.ResponseObjectFactory;
import datawave.webservice.result.EventQueryResponseBase;

/**
 * <h1>Overview</h1> This is a query logic implementation that can handle delegating to a remote event query logic (i.e. one that returns an extension of
 * EventQueryResponseBase).
 */
public class RemoteEventQueryLogic extends BaseRemoteQueryLogic<EventBase> implements RemoteQueryLogic<EventBase> {

    protected static final Logger log = ThreadConfigurableLogger.getLogger(RemoteEventQueryLogic.class);

    /**
     * Basic constructor
     */
    public RemoteEventQueryLogic() {
        super();
    }

    /**
     * Copy constructor
     *
     * @param other
     *            - another RemoteEventQueryLogic object
     */
    public RemoteEventQueryLogic(RemoteEventQueryLogic other) {
        super(other);
    }

    @Override
    public void setupQuery(GenericQueryConfiguration genericConfig) throws Exception {
        setupConfig(genericConfig);

        // Create an iterator that returns a stream of EventBase objects
        iterator = new RemoteQueryLogicIterator();
    }

    @Override
    public QueryLogicTransformer<EventBase,EventBase> createTransformer(Query settings, MarkingFunctions markingFunctions,
                    ResponseObjectFactory responseObjectFactory) {
        return new EventBaseTransformer(settings, markingFunctions, responseObjectFactory);
    }

    @Override
    public RemoteEventQueryLogic clone() {
        return new RemoteEventQueryLogic(this);
    }

    @Override
    public Set<String> getOptionalQueryParameters() {
        return new ShardQueryLogic().getOptionalQueryParameters();
    }

    @Override
    public Set<String> getRequiredQueryParameters() {
        return new ShardQueryLogic().getRequiredQueryParameters();
    }

    @Override
    public Set<String> getExampleQueries() {
        return new ShardQueryLogic().getExampleQueries();
    }

    private class RemoteQueryLogicIterator implements Iterator<EventBase> {
        private Queue<EventBase> data = new LinkedList<>();
        private boolean complete = false;

        @Override
        public boolean hasNext() {
            if (data.isEmpty() && !complete) {
                try {
                    EventQueryResponseBase response = (EventQueryResponseBase) remoteQueryService.next(getRemoteId(), getCallerObject());
                    if (response != null) {
                        if (response.getReturnedEvents() == 0) {
                            if (response.isPartialResults()) {
                                EventBase e = responseObjectFactory.getEvent();
                                e.setIntermediateResult(true);
                                data.add(e);
                            } else {
                                complete = true;
                            }
                        } else {
                            for (EventBase event : response.getEvents()) {
                                data.add(event);
                            }
                        }
                    } else {
                        // in this case we must have gotten a 204, so we are done
                        complete = true;
                    }
                } catch (Exception e) {
                    complete = true;
                    throw new RuntimeException(e.getMessage(), e);
                }
            }
            return !data.isEmpty();
        }

        @Override
        public EventBase next() {
            return data.poll();
        }
    }

    private class EventBaseTransformer extends EventQueryTransformerSupport<EventBase,EventBase> {

        public EventBaseTransformer(Query settings, MarkingFunctions markingFunctions, ResponseObjectFactory responseObjectFactory) {
            super("notable", settings, markingFunctions, responseObjectFactory);
        }

        public EventBaseTransformer(BaseQueryLogic<Map.Entry<Key,Value>> logic, Query settings, MarkingFunctions markingFunctions,
                        ResponseObjectFactory responseObjectFactory) {
            super(logic, settings, markingFunctions, responseObjectFactory);
        }

        @Override
        public EventBase transform(EventBase input) throws EmptyObjectException {
            return input;
        }

    }
}

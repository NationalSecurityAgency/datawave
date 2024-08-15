package datawave.query.tables;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;

import org.apache.log4j.Logger;

import datawave.core.common.logging.ThreadConfigurableLogger;
import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.core.query.exception.EmptyObjectException;
import datawave.core.query.logic.QueryLogicTransformer;
import datawave.edge.model.EdgeModelFields;
import datawave.edge.model.EdgeModelFieldsFactory;
import datawave.marking.MarkingFunctions;
import datawave.microservice.query.Query;
import datawave.query.tables.edge.EdgeQueryLogic;
import datawave.query.tables.remote.RemoteQueryLogic;
import datawave.query.transformer.EdgeQueryTransformerSupport;
import datawave.webservice.query.result.EdgeQueryResponseBase;
import datawave.webservice.query.result.edge.EdgeBase;
import datawave.webservice.query.result.event.ResponseObjectFactory;

/**
 * <h1>Overview</h1> This is a query logic implementation that can handle delegating to a remote edge query logic (i.e. one that returns an extension of
 * EdgeQueryResponseBase).
 */
public class RemoteEdgeQueryLogic extends BaseRemoteQueryLogic<EdgeBase> implements RemoteQueryLogic<EdgeBase> {

    protected static final Logger log = ThreadConfigurableLogger.getLogger(RemoteEdgeQueryLogic.class);

    protected EdgeModelFields edgeFields;

    /**
     * Basic constructor
     */
    public RemoteEdgeQueryLogic() {
        super();
    }

    /**
     * Copy constructor
     *
     * @param other
     *            - another RemoteEdgeQueryLogic object
     */
    public RemoteEdgeQueryLogic(RemoteEdgeQueryLogic other) {
        super(other);
    }

    @Override
    public void setupQuery(GenericQueryConfiguration genericConfig) throws Exception {
        setupConfig(genericConfig);

        // Create an iterator that returns a stream of EdgeBase objects
        iterator = new RemoteQueryLogicIterator();
    }

    @Override
    public QueryLogicTransformer<EdgeBase,EdgeBase> createTransformer(Query settings, MarkingFunctions markingFunctions,
                    ResponseObjectFactory responseObjectFactory) {
        return new EdgeBaseTransformer(settings, markingFunctions, responseObjectFactory, edgeFields);
    }

    @Override
    public RemoteEdgeQueryLogic clone() {
        return new RemoteEdgeQueryLogic(this);
    }

    @Override
    public Set<String> getOptionalQueryParameters() {
        return new EdgeQueryLogic().getOptionalQueryParameters();
    }

    @Override
    public Set<String> getRequiredQueryParameters() {
        return new EdgeQueryLogic().getRequiredQueryParameters();
    }

    @Override
    public Set<String> getExampleQueries() {
        return new EdgeQueryLogic().getExampleQueries();
    }

    private class RemoteQueryLogicIterator implements Iterator<EdgeBase> {
        private Queue<EdgeBase> data = new LinkedList<>();
        private boolean complete = false;

        @Override
        public boolean hasNext() {
            if (data.isEmpty() && !complete) {
                try {
                    EdgeQueryResponseBase response = (EdgeQueryResponseBase) remoteQueryService.next(getRemoteId(), getCurrentUser());
                    if (response != null) {
                        if (response.getTotalResults() == 0) {
                            if (!response.isPartialResults()) {
                                complete = true;
                            }
                        } else {
                            for (EdgeBase edge : response.getEdges()) {
                                data.add(edge);
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
        public EdgeBase next() {
            return data.poll();
        }
    }

    public void setEdgeModelFieldsFactory(EdgeModelFieldsFactory edgeModelFieldsFactory) {
        this.edgeFields = edgeModelFieldsFactory.createFields();
    }

    public void setEdgeFields(EdgeModelFields edgeFields) {
        this.edgeFields = edgeFields;
    }

    public EdgeModelFields getEdgeFields() {
        return edgeFields;
    }

    private class EdgeBaseTransformer extends EdgeQueryTransformerSupport<EdgeBase,EdgeBase> {

        public EdgeBaseTransformer(Query settings, MarkingFunctions markingFunctions, ResponseObjectFactory responseObjectFactory, EdgeModelFields fields) {
            super(settings, markingFunctions, responseObjectFactory, fields);
        }

        @Override
        public EdgeBase transform(EdgeBase input) throws EmptyObjectException {
            return input;
        }

    }
}

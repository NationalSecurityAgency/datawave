package datawave.query.tables;

import java.util.Collections;
import java.util.Iterator;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.security.Authorizations;

import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.core.query.logic.DelegatingQueryLogic;
import datawave.core.query.logic.QueryLogic;
import datawave.core.query.remote.RemoteTimeoutQueryException;
import datawave.core.query.remote.RemoteTimeoutQueryRuntimeException;
import datawave.microservice.query.Query;

/**
 * This QueryLogic will intercept RemoteTimeoutException and RemoteTimeoutQueryRuntimeException and optionally suppress them. Calls made after a suppressed
 * timeout will be short-circuited.
 */
public class RemoteTimeoutInterceptingQueryLogic extends DelegatingQueryLogic implements QueryLogic<Object> {
    public static final String REMOTE_TIMEOUT_PLAN = "( plan = 'RemoteTimeoutQueryException' )";

    private boolean suppressTimeout;
    private transient boolean timedOut;

    public RemoteTimeoutInterceptingQueryLogic() {
        // no-op
    }

    public RemoteTimeoutInterceptingQueryLogic(RemoteTimeoutInterceptingQueryLogic other) throws CloneNotSupportedException {
        super(other);
        suppressTimeout = other.suppressTimeout;
    }

    @Override
    public String getPlan(AccumuloClient connection, Query settings, Set<Authorizations> runtimeQueryAuthorizations, boolean expandFields, boolean expandValues)
                    throws Exception {
        try {
            return super.getPlan(connection, settings, runtimeQueryAuthorizations, expandFields, expandValues);
        } catch (RemoteTimeoutQueryException e) {
            if (!suppressTimeout) {
                throw e;
            }
        }

        // op didn't complete
        return REMOTE_TIMEOUT_PLAN;
    }

    @Override
    public GenericQueryConfiguration initialize(AccumuloClient connection, Query settings, Set<Authorizations> runtimeQueryAuthorizations) throws Exception {
        try {
            return super.initialize(connection, settings, runtimeQueryAuthorizations);
        } catch (RemoteTimeoutQueryException e) {
            if (!suppressTimeout) {
                throw e;
            }
            timedOut = true;
        }

        // op didn't complete
        return null;
    }

    @Override
    public void setupQuery(GenericQueryConfiguration configuration) throws Exception {
        if (timedOut) {
            return;
        }

        try {
            super.setupQuery(configuration);
        } catch (RemoteTimeoutQueryException e) {
            if (!suppressTimeout) {
                throw e;
            }
            timedOut = true;
        }
    }

    @Override
    public Iterator<Object> iterator() {
        if (timedOut) {
            return Collections.emptyIterator();
        }

        final Iterator<Object> delegateIterator = super.iterator();
        return new Iterator<>() {
            @Override
            public boolean hasNext() {
                if (timedOut) {
                    return false;
                }

                try {
                    return delegateIterator.hasNext();
                } catch (RemoteTimeoutQueryRuntimeException e) {
                    if (suppressTimeout) {
                        timedOut = true;
                        return false;
                    }
                    throw e;
                }
            }

            @Override
            public Object next() {
                return delegateIterator.next();
            }
        };
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        return new RemoteTimeoutInterceptingQueryLogic(this);
    }

    public void setSuppressTimeout(boolean suppressTimeout) {
        this.suppressTimeout = suppressTimeout;
    }

    public boolean isSuppressTimeout() {
        return this.suppressTimeout;
    }
}

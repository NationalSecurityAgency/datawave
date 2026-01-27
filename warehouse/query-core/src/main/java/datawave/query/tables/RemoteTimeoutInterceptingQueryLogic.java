package datawave.query.tables;

import java.util.Collections;
import java.util.Iterator;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.collections4.Transformer;
import org.apache.commons.collections4.iterators.TransformIterator;
import org.apache.log4j.Logger;

import datawave.authorization.remote.RemoteAuthorizationException;
import datawave.core.common.logging.ThreadConfigurableLogger;
import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.core.query.logic.BaseQueryLogic;
import datawave.core.query.logic.DelegatingQueryLogic;
import datawave.core.query.logic.QueryLogic;
import datawave.core.query.remote.RemoteTimeoutQueryException;
import datawave.core.query.remote.RemoteTimeoutQueryRuntimeException;
import datawave.microservice.query.Query;
import datawave.security.authorization.AuthorizationException;
import datawave.security.authorization.ProxiedUserDetails;
import datawave.security.authorization.UserOperations;
import datawave.user.AuthorizationsListBase;
import datawave.user.DefaultAuthorizationsList;
import datawave.webservice.result.GenericResponse;

/**
 * This QueryLogic will intercept RemoteTimeoutException and RemoteTimeoutQueryRuntimeException and optionally suppress them. Calls made after a suppressed
 * timeout will be short-circuited.
 */
public class RemoteTimeoutInterceptingQueryLogic extends DelegatingQueryLogic implements QueryLogic<Object> {
    public static final String REMOTE_TIMEOUT_PLAN = "(RemoteTimeoutSuppressed = 'true')";
    private static final Logger log = ThreadConfigurableLogger.getLogger(RemoteTimeoutInterceptingQueryLogic.class);

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
            log.warn("remote timeout getting plan", e);
            if (!suppressTimeout) {
                throw e;
            }
        }

        // op didn't complete
        return REMOTE_TIMEOUT_PLAN;
    }

    @Override
    public GenericQueryConfiguration initialize(AccumuloClient connection, Query settings, Set<Authorizations> runtimeQueryAuthorizations) throws Exception {
        if (!timedOut) {
            try {
                return super.initialize(connection, settings, runtimeQueryAuthorizations);
            } catch (RemoteTimeoutQueryException e) {
                log.warn("remote timeout initializing", e);
                if (!suppressTimeout) {
                    throw e;
                }
                timedOut = true;
            }
        }

        // op didn't complete
        if (getDelegate() instanceof BaseQueryLogic) {
            GenericQueryConfiguration config = ((BaseQueryLogic<Object>) getDelegate()).getConfig();
            config.setQuery(settings);
            config.setQueryString(REMOTE_TIMEOUT_PLAN);
            return config;
        }

        // can't return isn't available
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
            log.warn("remote timeout setupQuery", e);
            if (!suppressTimeout) {
                throw e;
            }
            timedOut = true;
        }
    }

    @Override
    public TransformIterator getTransformIterator(Query settings) {
        if (timedOut) {
            return new TransformIterator() {
                @Override
                public boolean hasNext() {
                    return false;
                }
            };
        }

        final TransformIterator delegate = super.getTransformIterator(settings);

        // wrap the existing TransformIterator and catch timeouts
        return new TransformIterator() {
            @Override
            public boolean hasNext() {
                if (timedOut) {
                    return false;
                }

                try {
                    return delegate.hasNext();
                } catch (RemoteTimeoutQueryRuntimeException e) {
                    log.warn("remote timeout transform", e);
                    if (!suppressTimeout) {
                        throw e;
                    }
                    timedOut = true;
                    return false;
                }
            }

            @Override
            public Object next() {
                return delegate.next();
            }

            @Override
            public void remove() {
                delegate.remove();
            }

            @Override
            public Iterator getIterator() {
                return delegate.getIterator();
            }

            @Override
            public Transformer getTransformer() {
                return delegate.getTransformer();
            }

            @Override
            public void setTransformer(Transformer transformer) {
                delegate.setTransformer(transformer);
            }
        };
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
                    log.warn("remote timeout hasNext", e);
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
    public UserOperations getUserOperations() {
        final UserOperations base = super.getUserOperations();

        if (base == null) {
            return base;
        }

        return new UserOperations() {
            @Override
            public AuthorizationsListBase listEffectiveAuthorizations(ProxiedUserDetails callerObject) throws AuthorizationException {
                try {
                    return base.listEffectiveAuthorizations(callerObject);
                } catch (RemoteAuthorizationException e) {
                    log.warn("remote timeout listEffectiveAuths", e);
                    if (!suppressTimeout) {
                        throw e;
                    }
                    timedOut = true;
                    // default this to empty since it timed out
                    DefaultAuthorizationsList blankAuths = new DefaultAuthorizationsList();
                    blankAuths.setUserAuths(callerObject.getPrimaryUser().getDn().subjectDN(), callerObject.getPrimaryUser().getDn().issuerDN(),
                                    Collections.emptyList());
                    blankAuths.addMessage("Remote list effective authorizations timed out");
                    return blankAuths;
                }
            }

            @Override
            public GenericResponse<String> flushCachedCredentials(ProxiedUserDetails callerObject) throws AuthorizationException {
                try {
                    return base.flushCachedCredentials(callerObject);
                } catch (RemoteAuthorizationException e) {
                    log.warn("remote timeout flushCreds", e);
                    if (!suppressTimeout) {
                        throw e;
                    }
                    timedOut = true;
                    GenericResponse<String> response = new GenericResponse<>();
                    response.addMessage("Remote flush credentials timed out");
                    return response;
                }
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

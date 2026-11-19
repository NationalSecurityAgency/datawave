package datawave.query.tables;

import static datawave.query.tables.RemoteTimeoutInterceptingQueryLogic.REMOTE_TIMEOUT_PLAN;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.ignoreStubs;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockingDetails;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.security.Authorizations;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.core.query.logic.BaseQueryLogic;
import datawave.core.query.remote.RemoteTimeoutQueryException;
import datawave.core.query.remote.RemoteTimeoutQueryRuntimeException;
import datawave.microservice.query.Query;
import datawave.webservice.query.exception.QueryException;

public class RemoteTimeoutInterceptingQueryLogicTest {
    private RemoteTimeoutInterceptingQueryLogic logic;
    private BaseQueryLogic delegate;

    private AccumuloClient client;
    private Query settings;
    private Set<Authorizations> auths;
    private Iterator data;

    @BeforeEach
    public void setup() {
        delegate = mock(BaseQueryLogic.class);

        logic = new RemoteTimeoutInterceptingQueryLogic();
        logic.setDelegate(delegate);

        data = List.of("A", "B", "C", "D").iterator();
    }

    @Test
    public void defaultInitializeTest() throws Exception {
        doThrow(new RemoteTimeoutQueryException()).when(delegate).initialize(client, settings, auths);

        assertThrows(RemoteTimeoutQueryException.class, () -> logic.initialize(client, settings, auths));

        verify(delegate, times(1)).initialize(client, settings, auths);

        if (delegate != null && mockingDetails(delegate).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(delegate));
        }
    }

    @Test
    public void unsuppressedInitializeTest() throws Exception {
        logic.setSuppressTimeout(true);

        doThrow(new QueryException()).when(delegate).initialize(client, settings, auths);

        assertThrows(QueryException.class, () -> logic.initialize(client, settings, auths));

        verify(delegate, times(1)).initialize(client, settings, auths);

        if (delegate != null && mockingDetails(delegate).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(delegate));
        }
    }

    @Test
    public void suppressInitializeTest() throws Exception {
        logic.setSuppressTimeout(true);

        doThrow(new RemoteTimeoutQueryException()).when(delegate).initialize(client, settings, auths);
        doReturn(new GenericQueryConfiguration()).when(delegate).getConfig();

        GenericQueryConfiguration config = logic.initialize(client, settings, auths);

        assertEquals(REMOTE_TIMEOUT_PLAN, config.getQueryString());

        // this should not actually do anything
        logic.setupQuery(config);

        // neither should this
        Iterator itr = logic.iterator();

        verify(delegate, times(1)).initialize(client, settings, auths);
        verify(delegate, times(1)).getConfig();

        assertFalse(itr.hasNext());

        if (delegate != null && mockingDetails(delegate).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(delegate));
        }
    }

    @Test
    public void defaultSetupTest() throws Exception {
        GenericQueryConfiguration config = new GenericQueryConfiguration();

        doReturn(config).when(delegate).initialize(client, settings, auths);
        doThrow(new RemoteTimeoutQueryException()).when(delegate).setupQuery(config);

        logic.initialize(client, settings, auths);
        assertThrows(RemoteTimeoutQueryException.class, () -> logic.setupQuery(config));

        verify(delegate, times(1)).initialize(client, settings, auths);
        verify(delegate, times(1)).setupQuery(config);

        if (delegate != null && mockingDetails(delegate).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(delegate));
        }
    }

    @Test
    public void unsuppressedSetupTest() throws Exception {
        logic.setSuppressTimeout(true);

        GenericQueryConfiguration config = new GenericQueryConfiguration();

        doReturn(config).when(delegate).initialize(client, settings, auths);
        doThrow(new QueryException()).when(delegate).setupQuery(config);

        logic.initialize(client, settings, auths);
        assertThrows(QueryException.class, () -> logic.setupQuery(config));

        verify(delegate, times(1)).initialize(client, settings, auths);
        verify(delegate, times(1)).setupQuery(config);

        if (delegate != null && mockingDetails(delegate).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(delegate));
        }
    }

    @Test
    public void suppressSetupTest() throws Exception {
        logic.setSuppressTimeout(true);

        GenericQueryConfiguration config = new GenericQueryConfiguration();

        doReturn(config).when(delegate).initialize(client, settings, auths);
        doThrow(new RemoteTimeoutQueryException("exception")).when(delegate).setupQuery(config);

        logic.initialize(client, settings, auths);
        logic.setupQuery(config);

        // this should always be empty
        Iterator itr = logic.iterator();

        verify(delegate, times(1)).initialize(client, settings, auths);
        verify(delegate, times(1)).setupQuery(config);

        assertFalse(itr.hasNext());

        if (delegate != null && mockingDetails(delegate).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(delegate));
        }
    }

    @Test
    public void defaultIteratorTest() throws Exception {
        Iterator itr = new ExceptionThrowingIterator(data, 0, new RemoteTimeoutQueryRuntimeException("runtime exception"));

        GenericQueryConfiguration config = new GenericQueryConfiguration();

        doReturn(config).when(delegate).initialize(client, settings, auths);

        doReturn(itr).when(delegate).iterator();

        logic.initialize(client, settings, auths);
        logic.setupQuery(config);

        Iterator logicItr = logic.iterator();

        assertThrows(RemoteTimeoutQueryRuntimeException.class, logicItr::hasNext);

        verify(delegate, times(1)).initialize(client, settings, auths);
        verify(delegate, times(1)).iterator();
        verify(delegate, times(1)).setupQuery(config);

        if (delegate != null && mockingDetails(delegate).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(delegate));
        }
    }

    @Test
    public void unsuppressedIteratorTest() throws Exception {
        Iterator itr = new ExceptionThrowingIterator(data, 0, new RuntimeException());

        GenericQueryConfiguration config = new GenericQueryConfiguration();

        doReturn(config).when(delegate).initialize(client, settings, auths);

        doReturn(itr).when(delegate).iterator();

        logic.initialize(client, settings, auths);
        logic.setupQuery(config);

        Iterator logicItr = logic.iterator();

        assertThrows(RuntimeException.class, logicItr::hasNext);

        verify(delegate, times(1)).initialize(client, settings, auths);
        verify(delegate, times(1)).iterator();
        verify(delegate, times(1)).setupQuery(config);

        if (delegate != null && mockingDetails(delegate).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(delegate));
        }
    }

    @Test
    public void suppressIteratorTest() throws Exception {
        logic.setSuppressTimeout(true);

        Iterator itr = new ExceptionThrowingIterator(data, 0, new RemoteTimeoutQueryRuntimeException("runtime exception"));

        GenericQueryConfiguration config = new GenericQueryConfiguration();

        doReturn(config).when(delegate).initialize(client, settings, auths);

        doReturn(itr).when(delegate).iterator();

        logic.initialize(client, settings, auths);
        logic.setupQuery(config);
        Iterator logicItr = logic.iterator();

        assertFalse(logicItr.hasNext());
        assertFalse(logicItr.hasNext());

        verify(delegate, times(1)).initialize(client, settings, auths);
        verify(delegate, times(1)).iterator();
        verify(delegate, times(1)).setupQuery(config);

        if (delegate != null && mockingDetails(delegate).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(delegate));
        }
    }

    @Test
    public void suppressLaterIteratorTest() throws Exception {
        logic.setSuppressTimeout(true);

        Iterator itr = new ExceptionThrowingIterator(data, 1, new RemoteTimeoutQueryRuntimeException("runtime exception"));

        GenericQueryConfiguration config = new GenericQueryConfiguration();

        doReturn(config).when(delegate).initialize(client, settings, auths);

        doReturn(itr).when(delegate).iterator();

        logic.initialize(client, settings, auths);
        logic.setupQuery(config);
        Iterator logicItr = logic.iterator();

        assertTrue(logicItr.hasNext());
        logicItr.next();
        assertFalse(logicItr.hasNext());

        verify(delegate, times(1)).initialize(client, settings, auths);
        verify(delegate, times(1)).iterator();
        verify(delegate, times(1)).setupQuery(config);

        if (delegate != null && mockingDetails(delegate).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(delegate));
        }
    }

    @Test
    public void defaultPlanTest() throws Exception {
        doThrow(new RemoteTimeoutQueryException("exception")).when(delegate).getPlan(client, settings, auths, false, false);

        assertThrows(RemoteTimeoutQueryException.class, () -> logic.getPlan(client, settings, auths, false, false));

        verify(delegate, times(1)).getPlan(client, settings, auths, false, false);

        if (delegate != null && mockingDetails(delegate).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(delegate));
        }
    }

    @Test
    public void suppressPlanTest() throws Exception {
        logic.setSuppressTimeout(true);

        doThrow(new RemoteTimeoutQueryException("exception")).when(delegate).getPlan(client, settings, auths, false, false);

        String plan = logic.getPlan(client, settings, auths, false, false);
        assertEquals(REMOTE_TIMEOUT_PLAN, plan);

        verify(delegate, times(1)).getPlan(client, settings, auths, false, false);

        if (delegate != null && mockingDetails(delegate).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(delegate));
        }
    }

    @Test
    public void planSuppressDoesNotEffectQueryTest() throws Exception {
        logic.setSuppressTimeout(true);

        doThrow(new RemoteTimeoutQueryException("exception")).when(delegate).getPlan(client, settings, auths, false, false);
        doThrow(new RemoteTimeoutQueryException("second exception")).when(delegate).initialize(client, settings, auths);
        doReturn(new GenericQueryConfiguration()).when(delegate).getConfig();

        String plan = logic.getPlan(client, settings, auths, false, false);
        assertEquals(REMOTE_TIMEOUT_PLAN, plan);

        logic.initialize(client, settings, auths);

        verify(delegate, times(1)).getPlan(client, settings, auths, false, false);
        verify(delegate, times(1)).initialize(client, settings, auths);
        verify(delegate, times(1)).getConfig();

        if (delegate != null && mockingDetails(delegate).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(delegate));
        }
    }

    private static class ExceptionThrowingIterator<T> implements Iterator<T> {
        private final Iterator<T> delegate;
        private final int elementsBeforeException;
        private final RuntimeException exception;

        private int elementCount = 0;

        public ExceptionThrowingIterator(Iterator<T> delegate, int elementsBeforeException, RuntimeException exception) {
            this.delegate = delegate;
            this.elementsBeforeException = elementsBeforeException;
            this.exception = exception;
        }

        @Override
        public boolean hasNext() {
            if (elementCount >= elementsBeforeException) {
                throw exception;
            }

            return delegate.hasNext();
        }

        @Override
        public T next() {
            elementCount++;
            return delegate.next();
        }
    }
}

package datawave.query.tables;

import static datawave.query.tables.RemoteTimeoutInterceptingQueryLogic.REMOTE_TIMEOUT_PLAN;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.security.Authorizations;
import org.easymock.EasyMockSupport;
import org.junit.Before;
import org.junit.Test;

import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.core.query.logic.BaseQueryLogic;
import datawave.core.query.remote.RemoteTimeoutQueryException;
import datawave.core.query.remote.RemoteTimeoutQueryRuntimeException;
import datawave.microservice.query.Query;
import datawave.webservice.query.exception.QueryException;

public class RemoteTimeoutInterceptingQueryLogicTest extends EasyMockSupport {
    private RemoteTimeoutInterceptingQueryLogic logic;
    private BaseQueryLogic delegate;

    private AccumuloClient client;
    private Query settings;
    private Set<Authorizations> auths;
    private Iterator data;

    @Before
    public void setup() {
        delegate = createMock(BaseQueryLogic.class);

        logic = new RemoteTimeoutInterceptingQueryLogic();
        logic.setDelegate(delegate);

        data = List.of("A", "B", "C", "D").iterator();
    }

    @Test(expected = RemoteTimeoutQueryException.class)
    public void defaultInitializeTest() throws Exception {
        expect(delegate.initialize(client, settings, auths)).andThrow(new RemoteTimeoutQueryException());

        replayAll();

        logic.initialize(client, settings, auths);

        verifyAll();
    }

    @Test(expected = QueryException.class)
    public void unsuppressedInitializeTest() throws Exception {
        logic.setSuppressTimeout(true);

        expect(delegate.initialize(client, settings, auths)).andThrow(new QueryException());

        replayAll();

        logic.initialize(client, settings, auths);

        verifyAll();
    }

    @Test
    public void suppressInitializeTest() throws Exception {
        logic.setSuppressTimeout(true);

        expect(delegate.initialize(client, settings, auths)).andThrow(new RemoteTimeoutQueryException());
        expect(delegate.getConfig()).andReturn(new GenericQueryConfiguration());

        replayAll();

        GenericQueryConfiguration config = logic.initialize(client, settings, auths);

        assertEquals(REMOTE_TIMEOUT_PLAN, config.getQueryString());

        // this should not actually do anything
        logic.setupQuery(config);

        // neither should this
        Iterator itr = logic.iterator();

        verifyAll();

        assertFalse(itr.hasNext());
    }

    @Test(expected = RemoteTimeoutQueryException.class)
    public void defaultSetupTest() throws Exception {
        GenericQueryConfiguration config = new GenericQueryConfiguration();

        expect(delegate.initialize(client, settings, auths)).andReturn(config);
        delegate.setupQuery(config);
        expectLastCall().andThrow(new RemoteTimeoutQueryException());

        replayAll();

        logic.initialize(client, settings, auths);
        logic.setupQuery(config);

        verifyAll();
    }

    @Test(expected = QueryException.class)
    public void unsuppressedSetupTest() throws Exception {
        logic.setSuppressTimeout(true);

        GenericQueryConfiguration config = new GenericQueryConfiguration();

        expect(delegate.initialize(client, settings, auths)).andReturn(config);
        delegate.setupQuery(config);
        expectLastCall().andThrow(new QueryException());

        replayAll();

        logic.initialize(client, settings, auths);
        logic.setupQuery(config);

        verifyAll();
    }

    @Test
    public void suppressSetupTest() throws Exception {
        logic.setSuppressTimeout(true);

        GenericQueryConfiguration config = new GenericQueryConfiguration();

        expect(delegate.initialize(client, settings, auths)).andReturn(config);
        delegate.setupQuery(config);
        expectLastCall().andThrow(new RemoteTimeoutQueryException("exception"));

        replayAll();

        logic.initialize(client, settings, auths);
        logic.setupQuery(config);

        // this should always be empty
        Iterator itr = logic.iterator();

        verifyAll();

        assertFalse(itr.hasNext());
    }

    @Test(expected = RemoteTimeoutQueryRuntimeException.class)
    public void defaultIteratorTest() throws Exception {
        Iterator itr = new ExceptionThrowingIterator(data, 0, new RemoteTimeoutQueryRuntimeException("runtime exception"));

        GenericQueryConfiguration config = new GenericQueryConfiguration();

        expect(delegate.initialize(client, settings, auths)).andReturn(config);
        delegate.setupQuery(config);
        expect(delegate.iterator()).andReturn(itr);

        replayAll();

        logic.initialize(client, settings, auths);
        logic.setupQuery(config);

        Iterator logicItr = logic.iterator();

        logicItr.hasNext();

        verifyAll();
    }

    @Test(expected = RuntimeException.class)
    public void unsuppressedIteratorTest() throws Exception {
        Iterator itr = new ExceptionThrowingIterator(data, 0, new RuntimeException());

        GenericQueryConfiguration config = new GenericQueryConfiguration();

        expect(delegate.initialize(client, settings, auths)).andReturn(config);
        delegate.setupQuery(config);
        expect(delegate.iterator()).andReturn(itr);

        replayAll();

        logic.initialize(client, settings, auths);
        logic.setupQuery(config);

        Iterator logicItr = logic.iterator();

        logicItr.hasNext();

        verifyAll();
    }

    @Test
    public void suppressIteratorTest() throws Exception {
        logic.setSuppressTimeout(true);

        Iterator itr = new ExceptionThrowingIterator(data, 0, new RemoteTimeoutQueryRuntimeException("runtime exception"));

        GenericQueryConfiguration config = new GenericQueryConfiguration();

        expect(delegate.initialize(client, settings, auths)).andReturn(config);
        delegate.setupQuery(config);
        expect(delegate.iterator()).andReturn(itr);

        replayAll();

        logic.initialize(client, settings, auths);
        logic.setupQuery(config);
        Iterator logicItr = logic.iterator();

        assertFalse(logicItr.hasNext());
        assertFalse(logicItr.hasNext());

        verifyAll();
    }

    @Test
    public void suppressLaterIteratorTest() throws Exception {
        logic.setSuppressTimeout(true);

        Iterator itr = new ExceptionThrowingIterator(data, 1, new RemoteTimeoutQueryRuntimeException("runtime exception"));

        GenericQueryConfiguration config = new GenericQueryConfiguration();

        expect(delegate.initialize(client, settings, auths)).andReturn(config);
        delegate.setupQuery(config);
        expect(delegate.iterator()).andReturn(itr);

        replayAll();

        logic.initialize(client, settings, auths);
        logic.setupQuery(config);
        Iterator logicItr = logic.iterator();

        assertTrue(logicItr.hasNext());
        logicItr.next();
        assertFalse(logicItr.hasNext());

        verifyAll();
    }

    @Test(expected = RemoteTimeoutQueryException.class)
    public void defaultPlanTest() throws Exception {
        expect(delegate.getPlan(client, settings, auths, false, false)).andThrow(new RemoteTimeoutQueryException("exception"));

        replayAll();

        logic.getPlan(client, settings, auths, false, false);

        verifyAll();
    }

    @Test
    public void suppressPlanTest() throws Exception {
        logic.setSuppressTimeout(true);

        expect(delegate.getPlan(client, settings, auths, false, false)).andThrow(new RemoteTimeoutQueryException("exception"));

        replayAll();

        String plan = logic.getPlan(client, settings, auths, false, false);
        assertEquals(REMOTE_TIMEOUT_PLAN, plan);

        verifyAll();
    }

    @Test
    public void planSuppressDoesNotEffectQueryTest() throws Exception {
        logic.setSuppressTimeout(true);

        expect(delegate.getPlan(client, settings, auths, false, false)).andThrow(new RemoteTimeoutQueryException("exception"));
        expect(delegate.initialize(client, settings, auths)).andThrow(new RemoteTimeoutQueryException("second exception"));
        expect(delegate.getConfig()).andReturn(new GenericQueryConfiguration());

        replayAll();

        String plan = logic.getPlan(client, settings, auths, false, false);
        assertEquals(REMOTE_TIMEOUT_PLAN, plan);

        logic.initialize(client, settings, auths);

        verifyAll();
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

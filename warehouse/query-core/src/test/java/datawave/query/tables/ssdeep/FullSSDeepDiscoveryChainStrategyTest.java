package datawave.query.tables.ssdeep;

import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.accumulo.core.client.AccumuloClient;
import org.easymock.Capture;
import org.easymock.EasyMockSupport;
import org.glassfish.jersey.internal.guava.Iterators;
import org.junit.Before;
import org.junit.Test;

import datawave.core.query.logic.QueryLogic;
import datawave.microservice.query.Query;
import datawave.microservice.query.QueryImpl;
import datawave.query.discovery.DiscoveredThing;
import datawave.util.ssdeep.SSDeepHash;

public class FullSSDeepDiscoveryChainStrategyTest extends EasyMockSupport {
    private AccumuloClient mockAccumulo;
    private QueryLogic mockLogic;
    private List<ScoredSSDeepPair> input;

    private Query settings;

    @Before
    public void setup() {
        mockAccumulo = createMock(AccumuloClient.class);
        mockLogic = createMock(QueryLogic.class);

        expect(mockLogic.getLogicName()).andReturn("secondLogic").anyTimes();

        input = new ArrayList<>();
        input.add(new ScoredSSDeepPair(new SSDeepHash(3, "abc", "def"), new SSDeepHash(3, "asdf", "gfs"), Collections.emptySet(), 234));

        settings = new QueryImpl();
    }

    @Test
    public void noInputTest() throws Exception {
        FullSSDeepDiscoveryChainStrategy strategy = new FullSSDeepDiscoveryChainStrategy();

        replayAll();

        Iterator<DiscoveredSSDeep> result = strategy.runChainedQuery(mockAccumulo, settings, null, Iterators.emptyIterator(), mockLogic);

        verifyAll();

        assertFalse(result.hasNext());
    }

    @Test
    public void singleInputTest() throws Exception {
        FullSSDeepDiscoveryChainStrategy strategy = new FullSSDeepDiscoveryChainStrategy();
        Capture<Query> intermediateSettings = Capture.newInstance();
        DiscoveredThing thing = createMock(DiscoveredThing.class);

        expect(mockLogic.initialize(eq(mockAccumulo), capture(intermediateSettings), eq(null))).andReturn(null);
        mockLogic.setupQuery(null);

        expect(mockLogic.iterator()).andReturn(List.of(new DiscoveredSSDeep(input.get(0), thing)).iterator());
        expect(thing.getTerm()).andReturn("3:asdf:gfs");

        replayAll();

        Iterator<DiscoveredSSDeep> result = strategy.runChainedQuery(mockAccumulo, settings, null, input.iterator(), mockLogic);

        verifyAll();

        assertEquals("CHECKSUM_SSDEEP:\"3:asdf:gfs\"", intermediateSettings.getValue().getQuery());

        assertTrue(result.hasNext());
        DiscoveredSSDeep next = result.next();
        assertEquals(234, next.getScoredSSDeepPair().getWeightedScore());
        assertFalse(result.hasNext());
    }

    @Test
    public void dualInputSingleBatchTest() throws Exception {
        input.add(new ScoredSSDeepPair(new SSDeepHash(3, "def", "def"), new SSDeepHash(3, "asdf", "gfq"), Collections.emptySet(), 101));

        FullSSDeepDiscoveryChainStrategy strategy = new FullSSDeepDiscoveryChainStrategy();
        Capture<Query> intermediateSettings = Capture.newInstance();
        DiscoveredThing thing = createMock(DiscoveredThing.class);

        expect(mockLogic.initialize(eq(mockAccumulo), capture(intermediateSettings), eq(null))).andReturn(null);
        mockLogic.setupQuery(null);

        expect(mockLogic.iterator()).andReturn(List.of(new DiscoveredSSDeep(input.get(0), thing)).iterator());
        expect(thing.getTerm()).andReturn("3:asdf:gfs");

        replayAll();

        Iterator<DiscoveredSSDeep> result = strategy.runChainedQuery(mockAccumulo, settings, null, input.iterator(), mockLogic);

        verifyAll();

        assertEquals("CHECKSUM_SSDEEP:\"3:asdf:gfs\" OR CHECKSUM_SSDEEP:\"3:asdf:gfq\"", intermediateSettings.getValue().getQuery());

        assertTrue(result.hasNext());
        DiscoveredSSDeep next = result.next();
        assertEquals(234, next.getScoredSSDeepPair().getWeightedScore());
        assertFalse(result.hasNext());
    }

    @Test
    public void dualInputDualBatchTest() throws Exception {
        input.add(new ScoredSSDeepPair(new SSDeepHash(3, "def", "def"), new SSDeepHash(3, "asdf", "gfq"), Collections.emptySet(), 101));

        FullSSDeepDiscoveryChainStrategy strategy = new FullSSDeepDiscoveryChainStrategy();
        strategy.setBatchSize(1);

        Capture<Query> intermediateSettings = Capture.newInstance();
        Capture<Query> intermediateSettings2 = Capture.newInstance();
        DiscoveredThing thing = createMock(DiscoveredThing.class);

        // first batch
        expect(mockLogic.initialize(eq(mockAccumulo), capture(intermediateSettings), eq(null))).andReturn(null);
        mockLogic.setupQuery(null);
        expect(mockLogic.iterator()).andReturn(List.of(new DiscoveredSSDeep(input.get(0), thing)).iterator());
        expect(thing.getTerm()).andReturn("3:asdf:gfs");

        // second batch
        expect(mockLogic.initialize(eq(mockAccumulo), capture(intermediateSettings2), eq(null))).andReturn(null);
        mockLogic.setupQuery(null);
        expect(mockLogic.iterator()).andReturn(Collections.emptyList().iterator());

        replayAll();

        Iterator<DiscoveredSSDeep> result = strategy.runChainedQuery(mockAccumulo, settings, null, input.iterator(), mockLogic);

        assertTrue(result.hasNext());
        DiscoveredSSDeep next = result.next();
        assertEquals(234, next.getScoredSSDeepPair().getWeightedScore());
        assertFalse(result.hasNext());

        verifyAll();

        assertEquals("CHECKSUM_SSDEEP:\"3:asdf:gfs\"", intermediateSettings.getValue().getQuery());
        assertEquals("CHECKSUM_SSDEEP:\"3:asdf:gfq\"", intermediateSettings2.getValue().getQuery());
    }

    @Test
    public void dupeInputIteratorTest() throws Exception {
        input.add(new ScoredSSDeepPair(new SSDeepHash(3, "abc", "def"), new SSDeepHash(3, "asdf", "gfs"), Collections.emptySet(), 234));
        input.add(new ScoredSSDeepPair(new SSDeepHash(3, "def", "def"), new SSDeepHash(3, "asdf", "gfq"), Collections.emptySet(), 101));

        FullSSDeepDiscoveryChainStrategy strategy = new FullSSDeepDiscoveryChainStrategy();
        strategy.setBatchSize(1);
        strategy.setDedupe(true);

        Capture<Query> intermediateSettings = Capture.newInstance();
        Capture<Query> intermediateSettings2 = Capture.newInstance();
        DiscoveredThing thing = createMock(DiscoveredThing.class);

        // first batch
        expect(mockLogic.initialize(eq(mockAccumulo), capture(intermediateSettings), eq(null))).andReturn(null);
        mockLogic.setupQuery(null);
        expect(mockLogic.iterator()).andReturn(List.of(new DiscoveredSSDeep(input.get(0), thing)).iterator());
        expect(thing.getTerm()).andReturn("3:asdf:gfs");

        // second batch
        expect(mockLogic.initialize(eq(mockAccumulo), capture(intermediateSettings2), eq(null))).andReturn(null);
        mockLogic.setupQuery(null);
        expect(mockLogic.iterator()).andReturn(Collections.emptyList().iterator());

        replayAll();

        Iterator<DiscoveredSSDeep> result = strategy.runChainedQuery(mockAccumulo, settings, null, input.iterator(), mockLogic);

        assertTrue(result.hasNext());
        DiscoveredSSDeep next = result.next();
        assertEquals(234, next.getScoredSSDeepPair().getWeightedScore());
        assertFalse(result.hasNext());

        verifyAll();

        assertEquals("CHECKSUM_SSDEEP:\"3:asdf:gfs\"", intermediateSettings.getValue().getQuery());
        assertEquals("CHECKSUM_SSDEEP:\"3:asdf:gfq\"", intermediateSettings2.getValue().getQuery());
    }

    @Test
    public void noDupeInputIteratorTest() throws Exception {
        input.add(new ScoredSSDeepPair(new SSDeepHash(3, "abc", "def"), new SSDeepHash(3, "asdf", "gfs"), Collections.emptySet(), 234));
        input.add(new ScoredSSDeepPair(new SSDeepHash(3, "def", "def"), new SSDeepHash(3, "asdf", "gfq"), Collections.emptySet(), 101));

        FullSSDeepDiscoveryChainStrategy strategy = new FullSSDeepDiscoveryChainStrategy();
        strategy.setBatchSize(1);
        strategy.setDedupe(false);

        Capture<Query> intermediateSettings = Capture.newInstance();
        Capture<Query> intermediateSettings2 = Capture.newInstance();
        Capture<Query> intermediateSettings3 = Capture.newInstance();
        DiscoveredThing thing = createMock(DiscoveredThing.class);

        // first batch
        expect(mockLogic.initialize(eq(mockAccumulo), capture(intermediateSettings), eq(null))).andReturn(null);
        mockLogic.setupQuery(null);
        expect(mockLogic.iterator()).andReturn(List.of(new DiscoveredSSDeep(input.get(0), thing)).iterator());
        expect(thing.getTerm()).andReturn("3:asdf:gfs");

        // second batch (dupe)
        expect(mockLogic.initialize(eq(mockAccumulo), capture(intermediateSettings2), eq(null))).andReturn(null);
        mockLogic.setupQuery(null);
        expect(mockLogic.iterator()).andReturn(List.of(new DiscoveredSSDeep(input.get(0), thing)).iterator());
        expect(thing.getTerm()).andReturn("3:asdf:gfs");

        // second batch
        expect(mockLogic.initialize(eq(mockAccumulo), capture(intermediateSettings3), eq(null))).andReturn(null);
        mockLogic.setupQuery(null);
        expect(mockLogic.iterator()).andReturn(Collections.emptyList().iterator());

        replayAll();

        Iterator<DiscoveredSSDeep> result = strategy.runChainedQuery(mockAccumulo, settings, null, input.iterator(), mockLogic);

        assertTrue(result.hasNext());
        DiscoveredSSDeep next = result.next();
        assertEquals(234, next.getScoredSSDeepPair().getWeightedScore());
        assertTrue(result.hasNext());
        next = result.next();
        assertEquals(234, next.getScoredSSDeepPair().getWeightedScore());
        assertFalse(result.hasNext());

        verifyAll();

        assertEquals("CHECKSUM_SSDEEP:\"3:asdf:gfs\"", intermediateSettings.getValue().getQuery());
        assertEquals("CHECKSUM_SSDEEP:\"3:asdf:gfs\"", intermediateSettings2.getValue().getQuery());
        assertEquals("CHECKSUM_SSDEEP:\"3:asdf:gfq\"", intermediateSettings3.getValue().getQuery());
    }
}

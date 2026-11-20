package datawave.query.tables.ssdeep;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.ignoreStubs;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockingDetails;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.accumulo.core.client.AccumuloClient;
import org.glassfish.jersey.internal.guava.Iterators;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import datawave.core.query.logic.QueryLogic;
import datawave.microservice.query.Query;
import datawave.microservice.query.QueryImpl;
import datawave.query.discovery.DiscoveredThing;
import datawave.util.ssdeep.SSDeepHash;

public class FullSSDeepDiscoveryChainStrategyTest {
    private AccumuloClient mockAccumulo;
    private QueryLogic mockLogic;
    private List<ScoredSSDeepPair> input;

    private Query settings;

    @BeforeEach
    public void setup() {
        mockAccumulo = mock(AccumuloClient.class);
        mockLogic = mock(QueryLogic.class);

        lenient().doReturn("secondLogic").when(mockLogic).getLogicName();

        input = new ArrayList<>();
        input.add(new ScoredSSDeepPair(new SSDeepHash(3, "abc", "def"), new SSDeepHash(3, "asdf", "gfs"), Collections.emptySet(), 234));

        settings = new QueryImpl();
    }

    @Test
    public void noInputTest() throws Exception {
        FullSSDeepDiscoveryChainStrategy strategy = new FullSSDeepDiscoveryChainStrategy();

        Iterator<DiscoveredSSDeep> result = strategy.runChainedQuery(mockAccumulo, settings, null, Iterators.emptyIterator(), mockLogic);

        assertFalse(result.hasNext());

        if (mockAccumulo != null && mockingDetails(mockAccumulo).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(mockAccumulo));
        }
        if (mockLogic != null && mockingDetails(mockLogic).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(mockLogic));
        }
    }

    @Test
    public void singleInputTest() throws Exception {
        FullSSDeepDiscoveryChainStrategy strategy = new FullSSDeepDiscoveryChainStrategy();
        ArgumentCaptor<Query> intermediateSettings = ArgumentCaptor.forClass(Query.class);
        DiscoveredThing thing = mock(DiscoveredThing.class);

        doReturn(List.of(new DiscoveredSSDeep(input.get(0), thing)).iterator()).when(mockLogic).iterator();
        doReturn("3:asdf:gfs").when(thing).getTerm();

        Iterator<DiscoveredSSDeep> result = strategy.runChainedQuery(mockAccumulo, settings, null, input.iterator(), mockLogic);

        verify(mockLogic, times(1)).initialize(eq(mockAccumulo), intermediateSettings.capture(), eq(null));
        verify(mockLogic, times(1)).iterator();
        verify(thing, times(1)).getTerm();
        verify(mockLogic, times(1)).setupQuery(null);

        assertEquals("CHECKSUM_SSDEEP:\"3:asdf:gfs\"", intermediateSettings.getValue().getQuery());

        assertTrue(result.hasNext());
        DiscoveredSSDeep next = result.next();
        assertEquals(234, next.getScoredSSDeepPair().getWeightedScore());
        assertFalse(result.hasNext());

        if (mockAccumulo != null && mockingDetails(mockAccumulo).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(mockAccumulo));
        }
        if (mockLogic != null && mockingDetails(mockLogic).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(mockLogic));
        }
        if (thing != null && mockingDetails(thing).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(thing));
        }
    }

    @Test
    public void dualInputSingleBatchTest() throws Exception {
        input.add(new ScoredSSDeepPair(new SSDeepHash(3, "def", "def"), new SSDeepHash(3, "asdf", "gfq"), Collections.emptySet(), 101));

        FullSSDeepDiscoveryChainStrategy strategy = new FullSSDeepDiscoveryChainStrategy();
        ArgumentCaptor<Query> intermediateSettings = ArgumentCaptor.forClass(Query.class);
        DiscoveredThing thing = mock(DiscoveredThing.class);

        doReturn(List.of(new DiscoveredSSDeep(input.get(0), thing)).iterator()).when(mockLogic).iterator();
        doReturn("3:asdf:gfs").when(thing).getTerm();

        Iterator<DiscoveredSSDeep> result = strategy.runChainedQuery(mockAccumulo, settings, null, input.iterator(), mockLogic);

        verify(mockLogic, times(1)).initialize(eq(mockAccumulo), intermediateSettings.capture(), eq(null));
        verify(mockLogic, times(1)).iterator();
        verify(thing, times(1)).getTerm();
        verify(mockLogic, times(1)).setupQuery(null);

        assertEquals("CHECKSUM_SSDEEP:\"3:asdf:gfs\" OR CHECKSUM_SSDEEP:\"3:asdf:gfq\"", intermediateSettings.getValue().getQuery());

        assertTrue(result.hasNext());
        DiscoveredSSDeep next = result.next();
        assertEquals(234, next.getScoredSSDeepPair().getWeightedScore());
        assertFalse(result.hasNext());

        if (mockAccumulo != null && mockingDetails(mockAccumulo).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(mockAccumulo));
        }
        if (mockLogic != null && mockingDetails(mockLogic).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(mockLogic));
        }
        if (thing != null && mockingDetails(thing).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(thing));
        }
    }

    @Test
    public void dualInputDualBatchTest() throws Exception {
        input.add(new ScoredSSDeepPair(new SSDeepHash(3, "def", "def"), new SSDeepHash(3, "asdf", "gfq"), Collections.emptySet(), 101));

        FullSSDeepDiscoveryChainStrategy strategy = new FullSSDeepDiscoveryChainStrategy();
        strategy.setBatchSize(1);

        ArgumentCaptor<Query> intermediateSettings = ArgumentCaptor.forClass(Query.class);
        DiscoveredThing thing = mock(DiscoveredThing.class);

        // first batch

        doReturn(List.of(new DiscoveredSSDeep(input.get(0), thing)).iterator(), Collections.emptyList().iterator()).when(mockLogic).iterator();
        doReturn("3:asdf:gfs").when(thing).getTerm();

        // second batch

        Iterator<DiscoveredSSDeep> result = strategy.runChainedQuery(mockAccumulo, settings, null, input.iterator(), mockLogic);

        assertTrue(result.hasNext());
        DiscoveredSSDeep next = result.next();
        assertEquals(234, next.getScoredSSDeepPair().getWeightedScore());
        assertFalse(result.hasNext());

        verify(mockLogic, times(2)).iterator();
        verify(thing, times(1)).getTerm();
        verify(mockLogic, times(2)).setupQuery(null);
        verify(mockLogic, times(2)).initialize(eq(mockAccumulo), intermediateSettings.capture(), eq(null));

        assertEquals("CHECKSUM_SSDEEP:\"3:asdf:gfs\"", intermediateSettings.getAllValues().get(0).getQuery());
        assertEquals("CHECKSUM_SSDEEP:\"3:asdf:gfq\"", intermediateSettings.getAllValues().get(1).getQuery());

        if (mockAccumulo != null && mockingDetails(mockAccumulo).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(mockAccumulo));
        }
        if (mockLogic != null && mockingDetails(mockLogic).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(mockLogic));
        }
        if (thing != null && mockingDetails(thing).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(thing));
        }
    }

    @Test
    public void dupeInputIteratorTest() throws Exception {
        input.add(new ScoredSSDeepPair(new SSDeepHash(3, "abc", "def"), new SSDeepHash(3, "asdf", "gfs"), Collections.emptySet(), 234));
        input.add(new ScoredSSDeepPair(new SSDeepHash(3, "def", "def"), new SSDeepHash(3, "asdf", "gfq"), Collections.emptySet(), 101));

        FullSSDeepDiscoveryChainStrategy strategy = new FullSSDeepDiscoveryChainStrategy();
        strategy.setBatchSize(1);
        strategy.setDedupe(true);

        ArgumentCaptor<Query> intermediateSettings = ArgumentCaptor.forClass(Query.class);
        DiscoveredThing thing = mock(DiscoveredThing.class);

        // first batch

        doReturn(List.of(new DiscoveredSSDeep(input.get(0), thing)).iterator(), Collections.emptyList().iterator()).when(mockLogic).iterator();
        doReturn("3:asdf:gfs").when(thing).getTerm();

        // second batch

        Iterator<DiscoveredSSDeep> result = strategy.runChainedQuery(mockAccumulo, settings, null, input.iterator(), mockLogic);

        assertTrue(result.hasNext());
        DiscoveredSSDeep next = result.next();
        assertEquals(234, next.getScoredSSDeepPair().getWeightedScore());
        assertFalse(result.hasNext());

        verify(mockLogic, times(2)).iterator();
        verify(thing, times(1)).getTerm();
        verify(mockLogic, times(2)).setupQuery(null);
        verify(mockLogic, times(2)).initialize(eq(mockAccumulo), intermediateSettings.capture(), eq(null));

        assertEquals("CHECKSUM_SSDEEP:\"3:asdf:gfs\"", intermediateSettings.getAllValues().get(0).getQuery());
        assertEquals("CHECKSUM_SSDEEP:\"3:asdf:gfq\"", intermediateSettings.getAllValues().get(1).getQuery());

        if (mockAccumulo != null && mockingDetails(mockAccumulo).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(mockAccumulo));
        }
        if (mockLogic != null && mockingDetails(mockLogic).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(mockLogic));
        }
        if (thing != null && mockingDetails(thing).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(thing));
        }
    }

    @Test
    public void noDupeInputIteratorTest() throws Exception {
        input.add(new ScoredSSDeepPair(new SSDeepHash(3, "abc", "def"), new SSDeepHash(3, "asdf", "gfs"), Collections.emptySet(), 234));
        input.add(new ScoredSSDeepPair(new SSDeepHash(3, "def", "def"), new SSDeepHash(3, "asdf", "gfq"), Collections.emptySet(), 101));

        FullSSDeepDiscoveryChainStrategy strategy = new FullSSDeepDiscoveryChainStrategy();
        strategy.setBatchSize(1);
        strategy.setDedupe(false);

        ArgumentCaptor<Query> intermediateSettings = ArgumentCaptor.forClass(Query.class);
        DiscoveredThing thing = mock(DiscoveredThing.class);

        // first batch

        doReturn(List.of(new DiscoveredSSDeep(input.get(0), thing)).iterator(), List.of(new DiscoveredSSDeep(input.get(0), thing)).iterator(),
                        Collections.emptyList().iterator()).when(mockLogic).iterator();
        doReturn("3:asdf:gfs", "3:asdf:gfs").when(thing).getTerm();

        // second batch (dupe)

        // second batch

        Iterator<DiscoveredSSDeep> result = strategy.runChainedQuery(mockAccumulo, settings, null, input.iterator(), mockLogic);

        assertTrue(result.hasNext());
        DiscoveredSSDeep next = result.next();
        assertEquals(234, next.getScoredSSDeepPair().getWeightedScore());
        assertTrue(result.hasNext());
        next = result.next();
        assertEquals(234, next.getScoredSSDeepPair().getWeightedScore());
        assertFalse(result.hasNext());

        verify(mockLogic, times(3)).iterator();
        verify(thing, times(2)).getTerm();
        verify(mockLogic, times(3)).setupQuery(null);
        verify(mockLogic, times(3)).initialize(eq(mockAccumulo), intermediateSettings.capture(), eq(null));

        assertEquals("CHECKSUM_SSDEEP:\"3:asdf:gfs\"", intermediateSettings.getAllValues().get(0).getQuery());
        assertEquals("CHECKSUM_SSDEEP:\"3:asdf:gfs\"", intermediateSettings.getAllValues().get(1).getQuery());
        assertEquals("CHECKSUM_SSDEEP:\"3:asdf:gfq\"", intermediateSettings.getAllValues().get(2).getQuery());

        if (mockAccumulo != null && mockingDetails(mockAccumulo).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(mockAccumulo));
        }
        if (mockLogic != null && mockingDetails(mockLogic).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(mockLogic));
        }
        if (thing != null && mockingDetails(thing).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(thing));
        }
    }
}

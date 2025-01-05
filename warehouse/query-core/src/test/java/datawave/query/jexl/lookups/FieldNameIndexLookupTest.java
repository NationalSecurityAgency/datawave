package datawave.query.jexl.lookups;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.isA;

import java.lang.reflect.InvocationTargetException;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.easymock.EasyMockSupport;
import org.junit.Before;
import org.junit.Test;

import datawave.microservice.query.QueryImpl;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.tables.AnyFieldScanner;
import datawave.query.tables.ScannerFactory;
import datawave.query.tables.SessionOptions;

public class FieldNameIndexLookupTest extends EasyMockSupport {
    private FieldNameIndexLookup lookup;
    private ShardQueryConfiguration config;
    private ScannerFactory scannerFactory;
    private ExecutorService executorService;

    private Set<String> fields = new HashSet<>();
    private Set<String> terms = new HashSet<>();

    @Before
    public void setup() {
        config = new ShardQueryConfiguration();
        scannerFactory = createMock(ScannerFactory.class);
        executorService = createMock(ExecutorService.class);

        fields = new HashSet<>();
        terms = new HashSet<>();
    }

    @Test
    public void initTest() {
        lookup = new FieldNameIndexLookup(config, scannerFactory, fields, terms, executorService);
    }

    @Test(expected = RuntimeException.class)
    @SuppressWarnings({"unchecked", "ConstantConditions"})
    public void submitErrorEnsureCloseTest() throws InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        AnyFieldScanner scannerSession = createMock(AnyFieldScanner.class);

        Date begin = new Date();
        Date end = new Date();
        config.setBeginDate(begin);
        config.setEndDate(end);

        fields.add("f1");
        fields.add("f2");
        terms.add("lookMeUp");
        lookup = new FieldNameIndexLookup(config, scannerFactory, fields, terms, executorService);

        expect(scannerFactory.newLimitedScanner(isA(Class.class), isA(String.class), isA(Set.class), isA(QueryImpl.class), isA(String.class)))
                        .andReturn(scannerSession).anyTimes();
        expect(scannerSession.setRanges(anyObject())).andReturn(scannerSession);
        expect(scannerSession.setOptions(anyObject())).andReturn(scannerSession);
        expect(scannerSession.getOptions()).andAnswer(SessionOptions::new).anyTimes();
        // this is sort of contrived, but necessary to test that the cleanup of the batch scanner would actually happen
        expect(executorService.submit(isA(Callable.class))).andThrow(new RuntimeException("testing"));
        scannerSession.close();

        replayAll();

        lookup.submit();

        verifyAll();
    }

    @Test
    @SuppressWarnings({"unchecked", "ConstantConditions"})
    public void timeoutTest() throws InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        AnyFieldScanner scannerSession = createMock(AnyFieldScanner.class);

        ExecutorService s = Executors.newSingleThreadExecutor();

        Date begin = new Date();
        Date end = new Date();
        config.setBeginDate(begin);
        config.setEndDate(end);

        config.setMaxAnyFieldScanTimeMillis(1);
        config.setMaxIndexScanTimeMillis(1);

        fields.add("f1");
        fields.add("f2");
        terms.add("lookMeUp");
        lookup = new FieldNameIndexLookup(config, scannerFactory, fields, terms, s);

        expect(scannerFactory.newLimitedScanner(isA(Class.class), isA(String.class), isA(Set.class), isA(QueryImpl.class), isA(String.class)))
                        .andReturn(scannerSession);
        expect(scannerSession.setRanges(anyObject())).andReturn(scannerSession);
        expect(scannerSession.setOptions(anyObject())).andReturn(scannerSession);
        expect(scannerSession.getOptions()).andAnswer(SessionOptions::new).anyTimes();

        expect(scannerSession.hasNext()).andAnswer(() -> {
            Thread.sleep(100000);
            return true;
        });

        scannerFactory.close(scannerSession);
        // once inside lookup() and another inside the runnable
        expectLastCall().times(2);

        replayAll();

        lookup.submit();
        lookup.lookup();

        verifyAll();
    }
}

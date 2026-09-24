package datawave.microservice.querymetric;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class BaseQueryMetricTest {

    static Stream<BaseQueryMetric.Lifecycle> finalStates() {
        return Stream.of(BaseQueryMetric.Lifecycle.CANCELLED, BaseQueryMetric.Lifecycle.MAXRESULTS, BaseQueryMetric.Lifecycle.MAXWORK,
                        BaseQueryMetric.Lifecycle.TIMEOUT, BaseQueryMetric.Lifecycle.NEXTTIMEOUT, BaseQueryMetric.Lifecycle.CLOSED);
    }

    static Stream<BaseQueryMetric.Lifecycle> otherStates() {
        return Stream.of(null, BaseQueryMetric.Lifecycle.NONE, BaseQueryMetric.Lifecycle.DEFINED, BaseQueryMetric.Lifecycle.INITIALIZED,
                        BaseQueryMetric.Lifecycle.RESULTS);
    }

    @ParameterizedTest(name = "{0} should be preserved")
    @MethodSource("finalStates")
    public void testFinalLifecycle(BaseQueryMetric.Lifecycle lifecycle) {
        BaseQueryMetric metric = new QueryMetric();
        metric.setLifecycle(lifecycle);
        assertEquals(lifecycle, metric.getLifecycle());
        metric.setLifecycle(BaseQueryMetric.Lifecycle.CLOSED);
        assertEquals(lifecycle, metric.getLifecycle());
    }

    @ParameterizedTest(name = "{0} should be over-written with CLOSE")
    @MethodSource("otherStates")
    public void testNonFinalLifecycles(BaseQueryMetric.Lifecycle lifecycle) {
        BaseQueryMetric metric = new QueryMetric();
        metric.setLifecycle(lifecycle);
        assertEquals(lifecycle, metric.getLifecycle());
        metric.setLifecycle(BaseQueryMetric.Lifecycle.CLOSED);
        assertEquals(BaseQueryMetric.Lifecycle.CLOSED, metric.getLifecycle());
    }

}

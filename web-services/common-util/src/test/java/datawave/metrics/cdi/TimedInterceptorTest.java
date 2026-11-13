package datawave.metrics.cdi;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

import com.codahale.metrics.annotation.Timed;

import jakarta.interceptor.InvocationContext;

public class TimedInterceptorTest {

    static class TimedMethods {
        @Timed(name = "dw.test.absolute", absolute = true)
        public void absolute() {}

        @Timed(name = "relative")
        public void relative() {}

        @Timed
        public void unnamed() {}
    }

    @Timed
    static class TimedClass {
        public void anyMethod() {}
    }

    @Test
    public void testTimerNames() throws Exception {
        assertEquals("dw.test.absolute", TimedInterceptor.timerName(TimedMethods.class.getMethod("absolute")));
        assertEquals(TimedMethods.class.getName() + ".relative", TimedInterceptor.timerName(TimedMethods.class.getMethod("relative")));
        assertEquals(TimedMethods.class.getName() + ".unnamed", TimedInterceptor.timerName(TimedMethods.class.getMethod("unnamed")));
        assertEquals(TimedClass.class.getName() + ".anyMethod", TimedInterceptor.timerName(TimedClass.class.getMethod("anyMethod")));
    }

    @Test
    public void testInvocationIsTimedInSharedRegistry() throws Exception {
        long before = DatawaveMetricRegistry.get().timer("dw.test.absolute").getCount();

        InvocationContext ctx = createMock(InvocationContext.class);
        expect(ctx.getMethod()).andReturn(TimedMethods.class.getMethod("absolute"));
        expect(ctx.proceed()).andReturn("result");
        replay(ctx);

        assertEquals("result", new TimedInterceptor().time(ctx));
        verify(ctx);
        assertEquals(before + 1, DatawaveMetricRegistry.get().timer("dw.test.absolute").getCount());
    }
}

package datawave.metrics.cdi;

import java.lang.reflect.Method;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.codahale.metrics.annotation.Timed;

import jakarta.annotation.Priority;
import jakarta.interceptor.AroundInvoke;
import jakarta.interceptor.Interceptor;
import jakarta.interceptor.InvocationContext;

/**
 * Times invocations of methods annotated with {@link Timed} (or of every business method of a class annotated with it) in the {@link DatawaveMetricRegistry},
 * naming the timer the way metrics-cdi did: the annotation's name when it is absolute, otherwise the name (or the method name) prefixed with the declaring
 * class name.
 */
@Timed
@Interceptor
@Priority(Interceptor.Priority.LIBRARY_BEFORE)
public class TimedInterceptor {

    @AroundInvoke
    public Object time(InvocationContext ctx) throws Exception {
        Timer timer = DatawaveMetricRegistry.get().timer(timerName(ctx.getMethod()));
        try (Timer.Context ignored = timer.time()) {
            return ctx.proceed();
        }
    }

    static String timerName(Method method) {
        Timed timed = method.getAnnotation(Timed.class);
        if (timed == null) {
            timed = method.getDeclaringClass().getAnnotation(Timed.class);
        }
        String name = (timed == null || timed.name().isEmpty()) ? method.getName() : timed.name();
        if (timed != null && timed.absolute()) {
            return name;
        }
        return MetricRegistry.name(method.getDeclaringClass(), name);
    }
}

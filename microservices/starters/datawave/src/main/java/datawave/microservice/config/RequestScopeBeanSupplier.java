package datawave.microservice.config;

import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aop.framework.Advised;

public class RequestScopeBeanSupplier<T> implements Supplier<T> {
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    private final T requestScopeBean;
    private final ThreadLocal<T> threadLocalOverride;
    
    public RequestScopeBeanSupplier(T requestScopeBean) {
        this.requestScopeBean = requestScopeBean;
        this.threadLocalOverride = new ThreadLocal<>();
    }
    
    @Override
    public T get() {
        if (threadLocalOverride.get() != null) {
            return threadLocalOverride.get();
        } else {
            // get the underlying object if this is a request-scoped bean
            if (requestScopeBean instanceof Advised) {
                try {
                    return (T) ((Advised) requestScopeBean).getTargetSource().getTarget();
                } catch (Exception e) {
                    log.warn("Unable to get target object for the request-scoped bean {}", requestScopeBean);
                }
            }
            return requestScopeBean;
        }
    }
    
    public ThreadLocal<T> getThreadLocalOverride() {
        return threadLocalOverride;
    }
}

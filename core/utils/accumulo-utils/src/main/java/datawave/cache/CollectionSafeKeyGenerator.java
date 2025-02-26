package datawave.cache;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.interceptor.KeyGenerator;
import org.springframework.cache.interceptor.SimpleKey;

/**
 * This is a key generator that can be used with a Cacheable spring annotation to create a key out of a methods parameters. This implementation will copy all
 * Collections to ensure that future concurrent modification exceptions do not occur of the collection passed is is subsequently reused.
 */
public class CollectionSafeKeyGenerator implements KeyGenerator {
    private static Logger log = LoggerFactory.getLogger(CollectionSafeKeyGenerator.class);
    
    @Override
    public Object generate(Object target, Method method, Object... params) {
        return generateKey(params);
    }
    
    /**
     * Generate a key based on the specified parameters, coping collections as appropriate
     */
    public static Object generateKey(Object... params) {
        if (params.length == 0) {
            return SimpleKey.EMPTY;
        }
        if (params.length == 1) {
            Object param = params[0];
            // based on SimpleKeyGenerator implementation
            if (param != null && !param.getClass().isArray()) {
                return copyIfCollectionParam(param);
            }
        }
        
        Object[] paramsCopy = new Object[params.length];
        for (int i = 0; i < params.length; i++) {
            paramsCopy[i] = copyIfCollectionParam(params[i]);
        }
        return new SimpleKey(paramsCopy);
    }
    
    /**
     * Copy a parameter iff it is a java.util collection or a map.
     * 
     * @param param
     * @return the param, or the param copy/clone if a collection or map
     */
    public static Object copyIfCollectionParam(Object param) {
        if (param instanceof Collection || param instanceof Map) {
            Class collClass = param.getClass();
            try {
                // look for cloneable
                if (param instanceof Cloneable) {
                    Method cloneMethod = collClass.getMethod("clone");
                    param = cloneMethod.invoke(param);
                }
                // look for a copy constructor
                else {
                    for (Constructor collConstructor : collClass.getConstructors()) {
                        Class<?>[] constructorParams = collConstructor.getParameterTypes();
                        if (constructorParams.length == 1 && constructorParams[0].isInstance(param)) {
                            param = collConstructor.newInstance(param);
                            break;
                        }
                    }
                }
            } catch (Exception e) {
                log.warn("Could not clone a " + collClass + " parameter for use in a Cacheable", e);
                // not cloneable or no copy constructor, so let it through as is
            }
        }
        return param;
    }
}

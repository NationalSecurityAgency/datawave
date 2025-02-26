package datawave.query.util;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;

import org.junit.Test;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.core.type.filter.AnnotationTypeFilter;

/**
 * Use spring to locate all @Cacheable annotations and validate that the key used matches available arguments and fields
 */
public class CachingConsistencyTest {
    @Test
    public void validateCacheableKeys() throws ClassNotFoundException, NoSuchFieldException {
        // grab all classes in the datawave.query classpath that have the Cacheable interface
        ClassPathScanningCandidateComponentProvider scanner = new ClassPathScanningCandidateComponentProvider(true);
        scanner.addIncludeFilter(new AnnotationTypeFilter(Cacheable.class));
        for (BeanDefinition bd : scanner.findCandidateComponents("datawave.query")) {
            Class<?> c = Class.forName(bd.getBeanClassName());
            // validate their key signature are consistent with their arguments and fields
            validateCacheableKeys(c.getDeclaredMethods());
        }
    }
    
    /**
     * Validate @Cacheable Methods have a key that is satisfied by the Fields and Parameters of the Method supports
     * <ul>
     * <li>#root.target.X where X is a class Field</li>
     * <li>#pX where X is the argument index</li>
     * <li>#argName where argName is the name of a Parameter passed to the Method</li>
     * </ul>
     *
     * @param methods
     * @throws NoSuchFieldException
     */
    private void validateCacheableKeys(Method[] methods) throws NoSuchFieldException {
        for (Method method : methods) {
            for (Annotation annotation : method.getAnnotations()) {
                if (annotation instanceof Cacheable) {
                    Cacheable c = (Cacheable) annotation;
                    String stripped = c.key().substring(1, c.key().length() - 1);
                    String[] keys = stripped.split(",");
                    
                    // for any arguments that don't start with root check that they match parameters in the method
                    for (String cacheKey : keys) {
                        if (!cacheKey.startsWith("#root")) {
                            if (cacheKey.startsWith("#p")) {
                                // validate based on argument count vs name
                                int argCount = Integer.parseInt(cacheKey.substring(2));
                                assertTrue("cacheKey: " + cacheKey + " doesn't have a matching argument in method: " + method,
                                                argCount < method.getParameters().length);
                            } else {
                                // had better match a param
                                boolean found = false;
                                for (Parameter p : method.getParameters()) {
                                    if (p.getName().equals(cacheKey.substring(1))) {
                                        found = true;
                                        break;
                                    }
                                }
                                
                                assertTrue("didn't find key parameter " + cacheKey + " in method:" + method, found);
                            }
                        } else {
                            String rootKey = cacheKey.substring(6);
                            if (rootKey.startsWith("target.")) {
                                // ensure the argument exists
                                Class<?> declaringClass = method.getDeclaringClass();
                                String targetFieldName = rootKey.substring(7);
                                boolean found = false;
                                for (Field field : declaringClass.getDeclaredFields()) {
                                    if (field.getName().equals(targetFieldName)) {
                                        found = true;
                                        break;
                                    }
                                }
                                assertTrue("could not locate defined field for key: " + cacheKey + " in method: " + method, found);
                            } else {
                                fail("unknown root key: " + cacheKey);
                            }
                        }
                    }
                }
            }
        }
    }
}

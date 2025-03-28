package datawave.marking;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.beanutils.BeanUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.collect.Maps;

/**
 * Accumulo marks all data with a columnVisibility that declares and controls access. MarkingFunctions provide a pattern for mapping a user's preferred means of
 * declaring access controls with the Accumulo columnVisibility pattern. As an example, James Bond might use MarkingFunctions to translate a the
 * columnVisibility FYEO into the song "For Your Eyes Only" A hospital might use a columnVisibility like 'PPI' but prefer to mark data input and results with a
 * human-readable marking like "Patient Privileged Information"
 */

public interface MarkingFunctions {
    
    ColumnVisibility combine(Collection<ColumnVisibility> columnVisibilities) throws MarkingFunctions.Exception;
    
    @SuppressWarnings("unchecked")
    Map<String,String> combine(Map<String,String>... markings) throws MarkingFunctions.Exception;
    
    ColumnVisibility translateToColumnVisibility(Map<String,String> markings) throws MarkingFunctions.Exception;
    
    Map<String,String> translateFromColumnVisibility(ColumnVisibility columnVisibility) throws MarkingFunctions.Exception;
    
    Map<String,String> translateFromColumnVisibilityForAuths(ColumnVisibility columnVisibility, Collection<Authorizations> authorizations)
                    throws MarkingFunctions.Exception;
    
    Map<String,String> translateFromColumnVisibilityForAuths(ColumnVisibility columnVisibility, Authorizations authorizations)
                    throws MarkingFunctions.Exception;
    
    byte[] flatten(ColumnVisibility vis);
    
    @SuppressWarnings("serial")
    class Exception extends java.lang.Exception {
        
        public Exception() {
            super();
        }
        
        public Exception(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
            super(message, cause, enableSuppression, writableStackTrace);
        }
        
        public Exception(String message, Throwable cause) {
            super(message, cause);
        }
        
        public Exception(String message) {
            super(message);
        }
        
        public Exception(Throwable cause) {
            super(cause);
        }
    }
    
    class Default implements MarkingFunctions {
        public static final String COLUMN_VISIBILITY = "columnVisibility";
        
        @Override
        public ColumnVisibility combine(Collection<ColumnVisibility> expressions) {
            
            // filter out any empty expressions, then flatten each one (to de-dupe) and concatenate with '&'
            // flatten the final combined ColumnVisibility and use that to make the ColumnVisibility to return
            return new ColumnVisibility(new ColumnVisibility(expressions.stream().map(ColumnVisibility::flatten).filter(b -> b.length > 0)
                            .map(b -> "(" + new String(b, UTF_8) + ")").collect(Collectors.joining("&")).getBytes(UTF_8)).flatten());
        }
        
        @Override
        @SafeVarargs
        public final Map<String,String> combine(Map<String,String>... markings) {
            // translate COLUMN_VISIBILITY values to ColumnVisibility, combine them and
            // return translated back to Map
            return translateFromColumnVisibility(combine(Arrays.stream(markings).filter(m -> m.containsKey(COLUMN_VISIBILITY))
                            .map(this::translateToColumnVisibility).collect(Collectors.toSet())));
        }
        
        @Override
        public ColumnVisibility translateToColumnVisibility(Map<String,String> markings) {
            ColumnVisibility cv = new ColumnVisibility(markings.get(COLUMN_VISIBILITY));
            return new ColumnVisibility(cv.flatten());
        }
        
        @Override
        public Map<String,String> translateFromColumnVisibility(ColumnVisibility expression) {
            Map<String,String> markings = Maps.newHashMap();
            markings.put(COLUMN_VISIBILITY, new String(expression.getExpression(), Charsets.UTF_8));
            return markings;
        }
        
        @Override
        public Map<String,String> translateFromColumnVisibilityForAuths(ColumnVisibility columnVisibility, Collection<Authorizations> authorizations) {
            return translateFromColumnVisibility(columnVisibility);
        }
        
        @Override
        public Map<String,String> translateFromColumnVisibilityForAuths(ColumnVisibility columnVisibility, Authorizations authorizations) {
            return translateFromColumnVisibility(columnVisibility);
        }
        
        @Override
        public byte[] flatten(ColumnVisibility vis) {
            return FlattenedVisibilityCache.flatten(vis);
        }
        
    }
    
    class Util {
        
        public static Object populate(Object obj, Map<String,String> source) {
            try {
                BeanUtils.populate(obj, source);
            } catch (IllegalAccessException | InvocationTargetException e) {
                throw new RuntimeException("Error populating object: " + obj.getClass().getName(), e);
            }
            return obj;
        }
    }
    
    class Encoding {
        static private Logger log = LoggerFactory.getLogger(Encoding.class);
        
        /**
         * Turn a set of markings into a serializable string
         * 
         * @param markings
         *            the markings map to convert to a string
         * @return a serialized String version of {@code markings}
         */
        public static String toString(Map<String,String> markings) {
            ObjectMapper objectMapper = new ObjectMapper();
            try {
                return objectMapper.writeValueAsString(markings);
            } catch (JsonProcessingException e) {
                log.error("could not serialize " + markings);
                return "";
            }
        }
        
        /**
         * Turn a serialized set of markings into a map
         * 
         * @param encodedMarkings
         *            the serialized String markings to convert back to a markings Map
         * @return a {@link Map} of the de-serialized markings from {@code encodedMarkings}
         */
        public static Map<String,String> fromString(String encodedMarkings) {
            ObjectMapper objectMapper = new ObjectMapper();
            try {
                // noinspection unchecked
                return objectMapper.readValue(encodedMarkings, Map.class);
            } catch (IOException e) {
                log.error("could not deserialize " + encodedMarkings);
                return Collections.emptyMap();
            }
        }
    }
    
    /**
     * this Factory for MarkingFunctions is designed to be used on the tservers, where there is a vfs-classloader
     */
    class Factory {
        public static final Logger log = LoggerFactory.getLogger(Factory.class);
        
        private static MarkingFunctions markingFunctions;
        
        public static synchronized MarkingFunctions createMarkingFunctions() {
            if (markingFunctions != null)
                return markingFunctions;
            ClassLoader thisClassLoader = Factory.class.getClassLoader();
            
            // ignore calls to close as this blows away the cache manager
            ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext();
            try {
                // To prevent failure when this is run on the tservers:
                // The VFS ClassLoader has been created and has been made the current thread's context classloader, but its resource paths are empty at this
                // time.
                // The spring ApplicationContext will prefer the current thread's context classloader, so the spring context would fail to find
                // any classes or context files to load.
                // Instead, set the classloader on the ApplicationContext to be the one that is loading this class.
                // It is a VFSClassLoader that has the accumulo lib/ext jars set as its resources.
                // After setting the classloader, then set the config locations and refresh the context.
                context.setClassLoader(thisClassLoader);
                context.setConfigLocations("classpath*:/MarkingFunctionsContext.xml");
                context.refresh();
                markingFunctions = context.getBean("markingFunctions", MarkingFunctions.class);
            } catch (Throwable t) {
                // got here because the VFSClassLoader on the tservers does not implement findResources
                // none of the spring wiring will work.
                log.warn("Could not load spring context files. got " + t);
            }
            
            return markingFunctions;
        }
    }
}

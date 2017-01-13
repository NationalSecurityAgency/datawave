package nsa.datawave.annotation;

import javax.ws.rs.NameBinding;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * A {@link NameBinding} to allow annotation of methods that are designated to clear the query session id cookie.
 *
 * @see nsa.datawave.annotation.GenerateQuerySessionId
 * @see nsa.datawave.resteasy.interceptor.ClearQuerySessionIDFilter
 */
@Target({ElementType.TYPE, ElementType.METHOD, ElementType.PARAMETER})
@Retention(RetentionPolicy.RUNTIME)
@Documented
@NameBinding
public @interface ClearQuerySessionId {
    
}

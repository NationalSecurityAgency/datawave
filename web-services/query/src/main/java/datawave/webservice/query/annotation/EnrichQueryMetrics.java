package datawave.webservice.query.annotation;

import javax.ws.rs.NameBinding;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target({ElementType.TYPE, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Documented
@NameBinding
public @interface EnrichQueryMetrics {
    enum MethodType {
        NONE, CREATE, NEXT, CREATE_AND_NEXT
    };

    MethodType methodType();
}

package datawave.microservice.annotation.util.lookup.config;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class LookupProperties {
    private static final DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
    private String datawaveQueryHost;
    private Class<?>[] datawaveResponseClasses;
    private boolean annotationUnknownFieldsIgnored;

    Date beginDate;
    Date endDate;

    {
        try {
            beginDate = dateFormat.parse("19700101");
            endDate = dateFormat.parse("30000101");
        } catch (java.text.ParseException e) {
            throw new ExceptionInInitializerError(e);
        }
    }
}

package datawave.resteasy.util;

import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import java.util.Date;

import org.jboss.resteasy.spi.util.FindAnnotation;

import datawave.annotation.DateFormat;
import jakarta.ws.rs.ext.ParamConverter;
import jakarta.ws.rs.ext.ParamConverterProvider;
import jakarta.ws.rs.ext.Provider;

@Provider
public class DateParamConverterProvider implements ParamConverterProvider {
    @SuppressWarnings("unchecked")
    @Override
    public <T> ParamConverter<T> getConverter(Class<T> rawType, Type genericType, Annotation[] annotations) {
        if (rawType.equals(Date.class)) {
            DateFormat format = FindAnnotation.findAnnotation(annotations, DateFormat.class);
            return (ParamConverter<T>) new DateFormatter(format);
        }
        return null;
    }
}

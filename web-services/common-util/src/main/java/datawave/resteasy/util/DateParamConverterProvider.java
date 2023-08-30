package datawave.resteasy.util;

import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import java.util.Date;

import javax.ws.rs.ext.ParamConverter;
import javax.ws.rs.ext.ParamConverterProvider;
import javax.ws.rs.ext.Provider;

import org.jboss.resteasy.util.FindAnnotation;

import datawave.annotation.DateFormat;

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

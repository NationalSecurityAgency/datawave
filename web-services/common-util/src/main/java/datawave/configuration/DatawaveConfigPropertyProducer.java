package datawave.configuration;

import org.apache.commons.lang.StringUtils;
import org.apache.deltaspike.core.api.config.ConfigProperty;
import org.apache.deltaspike.core.impl.config.DefaultConfigPropertyProducer;

import javax.annotation.Priority;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.context.Dependent;
import javax.enterprise.inject.Alternative;
import javax.enterprise.inject.Produces;
import javax.enterprise.inject.Specializes;
import javax.enterprise.inject.spi.InjectionPoint;
import javax.interceptor.Interceptor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Extensions to DeltaSpike's {@link ConfigProperty} to handle comma-separated property lists and maps.
 */
@Priority(Interceptor.Priority.APPLICATION)
// make this the default for the application
@Alternative
@Specializes
@ApplicationScoped
public class DatawaveConfigPropertyProducer extends DefaultConfigPropertyProducer {
    @Override
    @Alternative
    @Specializes
    @Produces
    @Dependent
    @ConfigProperty(name = "ignored")
    // we actually don't need the name
    public String produceStringConfiguration(InjectionPoint injectionPoint) {
        return super.produceStringConfiguration(injectionPoint);
    }
    
    @Override
    @Alternative
    @Specializes
    @Produces
    @Dependent
    @ConfigProperty(name = "ignored")
    // we actually don't need the name
    public Integer produceIntegerConfiguration(InjectionPoint injectionPoint) {
        return super.produceIntegerConfiguration(injectionPoint);
    }
    
    @Override
    @Alternative
    @Specializes
    @Produces
    @Dependent
    @ConfigProperty(name = "ignored")
    // we actually don't need the name
    public Long produceLongConfiguration(InjectionPoint injectionPoint) {
        return super.produceLongConfiguration(injectionPoint);
    }
    
    @Override
    @Alternative
    @Specializes
    @Produces
    @Dependent
    @ConfigProperty(name = "ignored")
    // we actually don't need the name
    public Boolean produceBooleanConfiguration(InjectionPoint injectionPoint) {
        return super.produceBooleanConfiguration(injectionPoint);
    }
    
    @Override
    @Alternative
    @Specializes
    @Produces
    @Dependent
    @ConfigProperty(name = "ignored")
    // we actually don't need the name
    public Float produceFloatConfiguration(InjectionPoint injectionPoint) {
        return super.produceFloatConfiguration(injectionPoint);
    }
    
    @Produces
    @Dependent
    @ConfigProperty(name = "ignored")
    // we actually don't need the name
    public List<String> produceStringListConfiguration(InjectionPoint injectionPoint) {
        String propertyValue = getStringPropertyValue(injectionPoint);
        String[] values = StringUtils.split(propertyValue, ",");
        return values == null ? Collections.emptyList() : Arrays.asList(values);
    }
    
    @Produces
    @Dependent
    @ConfigProperty(name = "ignored")
    // we actually don't need the name
    public Set<String> produceStringSetConfiguration(InjectionPoint injectionPoint) {
        String propertyValue = getStringPropertyValue(injectionPoint);
        String[] values = StringUtils.split(propertyValue, ",");
        return values == null ? Collections.emptySet() : new HashSet<>(Arrays.asList(values));
    }
    
    @Produces
    @Dependent
    @ConfigProperty(name = "ignored")
    // we actually don't need the name
    public List<Integer> produceIntegerListConfiguration(InjectionPoint injectionPoint) {
        String propertyValue = getStringPropertyValue(injectionPoint);
        String[] values = StringUtils.split(propertyValue, ",");
        
        ArrayList<Integer> list = new ArrayList<>();
        if (values != null) {
            for (String value : values) {
                try {
                    list.add(Integer.parseInt(value));
                } catch (NumberFormatException nfe) {
                    ConfigProperty configProperty = getAnnotation(injectionPoint, ConfigProperty.class);
                    throw new RuntimeException("Error while converting Integer property '" + configProperty.name() + "' value: " + value + " of "
                                    + propertyValue + " happening in bean " + injectionPoint.getBean(), nfe);
                }
                
            }
        }
        return list;
    }
    
    @Produces
    @Dependent
    @ConfigProperty(name = "ignored")
    // we actually don't need the name
    public List<Long> produceLongListConfiguration(InjectionPoint injectionPoint) {
        String propertyValue = getStringPropertyValue(injectionPoint);
        String[] values = StringUtils.split(propertyValue, ",");
        
        ArrayList<Long> list = new ArrayList<>();
        if (values != null) {
            for (String value : values) {
                try {
                    list.add(Long.parseLong(value));
                } catch (NumberFormatException nfe) {
                    ConfigProperty configProperty = getAnnotation(injectionPoint, ConfigProperty.class);
                    throw new RuntimeException("Error while converting Long property '" + configProperty.name() + "' value: " + value + " of " + propertyValue
                                    + " happening in bean " + injectionPoint.getBean(), nfe);
                }
                
            }
        }
        return list;
    }
    
    @Produces
    @Dependent
    @ConfigProperty(name = "ignored")
    // we actually don't need the name
    public List<Float> produceFloatListConfiguration(InjectionPoint injectionPoint) {
        String propertyValue = getStringPropertyValue(injectionPoint);
        String[] values = StringUtils.split(propertyValue, ",");
        
        ArrayList<Float> list = new ArrayList<>();
        if (values != null) {
            for (String value : values) {
                try {
                    list.add(Float.parseFloat(value));
                } catch (NumberFormatException nfe) {
                    ConfigProperty configProperty = getAnnotation(injectionPoint, ConfigProperty.class);
                    throw new RuntimeException("Error while converting Float property '" + configProperty.name() + "' value: " + value + " of " + propertyValue
                                    + " happening in bean " + injectionPoint.getBean(), nfe);
                }
                
            }
        }
        return list;
    }
    
    @Produces
    @Dependent
    @ConfigProperty(name = "ignored")
    // we actually don't need the name
    public List<Double> produceDoubleListConfiguration(InjectionPoint injectionPoint) {
        String propertyValue = getStringPropertyValue(injectionPoint);
        String[] values = StringUtils.split(propertyValue, ",");
        
        ArrayList<Double> list = new ArrayList<>();
        if (values != null) {
            for (String value : values) {
                try {
                    list.add(Double.parseDouble(value));
                } catch (NumberFormatException nfe) {
                    ConfigProperty configProperty = getAnnotation(injectionPoint, ConfigProperty.class);
                    throw new RuntimeException("Error while converting Double property '" + configProperty.name() + "' value: " + value + " of " + propertyValue
                                    + " happening in bean " + injectionPoint.getBean(), nfe);
                }
                
            }
        }
        return list;
    }
    
    @Produces
    @Dependent
    @ConfigProperty(name = "ignored")
    // we actually don't need the name
    public Map<String,String> produceStringStringMapConfiguration(InjectionPoint injectionPoint) {
        String propertyValue = getStringPropertyValue(injectionPoint);
        String[] pairs = StringUtils.split(propertyValue, "|");
        
        Map<String,String> map = new LinkedHashMap<>();
        if (pairs != null) {
            for (String pair : pairs) {
                String[] keyValue = StringUtils.split(pair, ";");
                if (keyValue != null && (keyValue.length == 1 || keyValue.length == 2)) {
                    map.put(keyValue[0], keyValue.length == 1 ? "" : keyValue[1]);
                } else {
                    ConfigProperty configProperty = getAnnotation(injectionPoint, ConfigProperty.class);
                    throw new RuntimeException("Error while converting Map<String,String> property '" + configProperty.name() + "' pair: " + pair + " of "
                                    + propertyValue + " happening in bean " + injectionPoint.getBean());
                }
            }
        }
        return map;
    }
    
    @Override
    protected String getPropertyValue(String propertyName, String defaultValue) {
        String value = super.getPropertyValue(propertyName, defaultValue);
        while (value != null && value.startsWith("$")) {
            String newPropertyName = value.substring(1);
            value = super.getPropertyValue(newPropertyName, ConfigProperty.NULL);
        }
        return value;
    }
}

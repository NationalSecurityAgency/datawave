package datawave.configuration;

import static datawave.webservice.common.audit.Auditor.AuditType;

import java.util.LinkedHashMap;
import java.util.Map;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.context.Dependent;
import javax.enterprise.inject.Produces;
import javax.enterprise.inject.spi.InjectionPoint;

import org.apache.commons.lang.StringUtils;
import org.apache.deltaspike.core.api.config.ConfigProperty;
import org.apache.deltaspike.core.spi.config.BaseConfigPropertyProducer;

/**
 * Extensions to DeltaSpike's {@link ConfigProperty} to handle comma-separated generation for some types that are defined here in the common library.
 *
 * @see datawave.configuration.DatawaveConfigPropertyProducer
 */
@ApplicationScoped
public class DatawaveCommonConfigPropertyProducer extends BaseConfigPropertyProducer {

    @Produces
    @Dependent
    @ConfigProperty(name = "ignored")
    // we actually don't need the name
    public Map<String,AuditType> produceStringAuditTypeMapConfiguration(InjectionPoint injectionPoint) {
        String propertyValue = getStringPropertyValue(injectionPoint);
        String[] pairs = StringUtils.split(propertyValue, "|");

        Map<String,AuditType> map = new LinkedHashMap<>();
        if (pairs != null) {
            for (String pair : pairs) {
                String[] keyValue = StringUtils.split(pair, ";");
                if (keyValue != null && keyValue.length == 2) {
                    map.put(keyValue[0], AuditType.valueOf(keyValue[1]));
                } else {
                    ConfigProperty configProperty = getAnnotation(injectionPoint, ConfigProperty.class);
                    throw new RuntimeException("Error while converting Map<String,AuditType> property '" + configProperty.name() + "' pair: " + pair + " of "
                                    + propertyValue + " happening in bean " + injectionPoint.getBean());
                }
            }
        }
        return map;
    }
}

package datawave.microservice.metadata.config;

import org.apache.accumulo.core.security.Authorizations;
import org.springframework.boot.context.properties.ConfigurationPropertiesBinding;
import org.springframework.core.convert.converter.Converter;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

/**
 * A spring boot property {@link Converter} that converts a comma-separated list of strings into an Accumulo {@link Authorizations} object.
 */
@Component
@ConfigurationPropertiesBinding
public class AuthorizationsConverter implements Converter<String,Authorizations> {
    @Override
    public Authorizations convert(String auths) {
        return new Authorizations(StringUtils.commaDelimitedListToStringArray(auths));
    }
}

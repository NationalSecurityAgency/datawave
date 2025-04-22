package datawave.microservice.config.web;

import static datawave.microservice.authorization.preauth.ProxiedEntityX509Filter.ENTITIES_HEADER;
import static datawave.microservice.authorization.preauth.ProxiedEntityX509Filter.ISSUERS_HEADER;
import static datawave.microservice.authorization.preauth.ProxiedEntityX509Filter.ISSUER_DN_HEADER;
import static datawave.microservice.authorization.preauth.ProxiedEntityX509Filter.SUBJECT_DN_HEADER;

import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.validation.Valid;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.Positive;
import javax.validation.constraints.PositiveOrZero;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.NestedConfigurationProperty;
import org.springframework.boot.web.server.Ssl;
import org.springframework.http.HttpHeaders;
import org.springframework.validation.annotation.Validated;

import com.google.common.collect.Lists;

import datawave.microservice.validator.NotBlankIfFieldEquals;

@Validated
@ConfigurationProperties(prefix = "server", ignoreInvalidFields = true)
public class DatawaveServerProperties {
    @PositiveOrZero
    private Integer nonSecurePort = null;
    
    @NotBlank
    private String cdnUri = "/";
    
    @Valid
    @NestedConfigurationProperty
    private Cors cors = new Cors();
    
    @Valid
    @NestedConfigurationProperty
    private OutboundSsl outboundSsl = new OutboundSsl();
    
    public Integer getNonSecurePort() {
        return nonSecurePort;
    }
    
    public void setNonSecurePort(Integer nonSecurePort) {
        this.nonSecurePort = nonSecurePort;
    }
    
    public String getCdnUri() {
        return cdnUri;
    }
    
    public void setCdnUri(String cdnUri) {
        this.cdnUri = cdnUri;
    }
    
    public Cors getCors() {
        return cors;
    }
    
    /**
     * Gets the {@link Ssl} configuration for outbound connections that may require two-way SSL. Note that this can be disabled by setting the property
     * "server.outbound-ssl.enabled" to "false".
     *
     * @return the outbound {@link Ssl} configuration
     */
    public OutboundSsl getOutboundSsl() {
        return outboundSsl;
    }
    
    public static class Cors {
        @NotEmpty
        private List<String> corsPaths = Lists.newArrayList("/**");
        @NotEmpty
        private String[] allowedOriginPatterns = new String[] {"*"};
        @NotEmpty
        private String[] allowedMethods = new String[] {"HEAD", "DELETE", "GET", "POST", "PUT", "OPTIONS"};
        private boolean allowCredentials = true;
        @NotEmpty
        private String[] allowedHeaders = new String[] {SUBJECT_DN_HEADER, ISSUER_DN_HEADER, ENTITIES_HEADER, ISSUERS_HEADER, HttpHeaders.ACCEPT,
                HttpHeaders.ACCEPT_ENCODING};
        @Positive
        private long maxAge = TimeUnit.DAYS.toSeconds(10);
        
        public List<String> getCorsPaths() {
            return corsPaths;
        }
        
        public void setCorsPaths(List<String> corsPaths) {
            this.corsPaths = corsPaths;
        }
        
        public String[] getAllowedOriginPatterns() {
            return allowedOriginPatterns;
        }
        
        public void setAllowedOriginPatterns(String[] allowedOriginPatterns) {
            this.allowedOriginPatterns = allowedOriginPatterns;
        }
        
        public String[] getAllowedMethods() {
            return allowedMethods;
        }
        
        public void setAllowedMethods(String[] allowedMethods) {
            this.allowedMethods = allowedMethods;
        }
        
        public String[] getAllowedHeaders() {
            return allowedHeaders;
        }
        
        public void setAllowedHeaders(String[] allowedHeaders) {
            this.allowedHeaders = allowedHeaders;
        }
        
        public boolean isAllowCredentials() {
            return allowCredentials;
        }
        
        public void setAllowCredentials(boolean allowCredentials) {
            this.allowCredentials = allowCredentials;
        }
        
        public long getMaxAge() {
            return maxAge;
        }
        
        public void setMaxAge(long maxAge) {
            this.maxAge = maxAge;
        }
    }
    
    @Validated
    @NotBlankIfFieldEquals.List({@NotBlankIfFieldEquals(fieldName = "enabled", fieldValue = "true", notBlankFieldName = "keyStore"),
            @NotBlankIfFieldEquals(fieldName = "enabled", fieldValue = "true", notBlankFieldName = "keyStorePassword"),
            @NotBlankIfFieldEquals(fieldName = "enabled", fieldValue = "true", notBlankFieldName = "keyStoreType"),
            @NotBlankIfFieldEquals(fieldName = "enabled", fieldValue = "true", notBlankFieldName = "trustStore"),
            @NotBlankIfFieldEquals(fieldName = "enabled", fieldValue = "true", notBlankFieldName = "trustStorePassword"),
            @NotBlankIfFieldEquals(fieldName = "enabled", fieldValue = "true", notBlankFieldName = "trustStoreType"),
            @NotBlankIfFieldEquals(fieldName = "enabled", fieldValue = "true", notBlankFieldName = "protocol")})
    public static class OutboundSsl extends Ssl {}
}

package datawave.microservice.modification.config;

import java.util.concurrent.TimeUnit;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Positive;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@ConfigurationProperties(prefix = "datawave.modification.query")
@Validated
public class ModificationQueryProperties {
    
    @NotBlank
    private String queryPool;
    
    @NotBlank
    private String queryURI;
    
    @Positive
    protected long remoteQueryTimeout = 1L;
    
    @NotNull
    protected TimeUnit remoteQueryTimeoutUnit = TimeUnit.MINUTES;
    
    public long getRemoteQueryTimeoutMillis() {
        return remoteQueryTimeoutUnit.toMillis(remoteQueryTimeout);
    }
    
}

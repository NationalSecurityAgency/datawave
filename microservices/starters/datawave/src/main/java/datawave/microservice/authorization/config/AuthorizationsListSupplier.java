package datawave.microservice.authorization.config;

import java.util.function.Supplier;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import datawave.user.AuthorizationsListBase;
import datawave.user.DefaultAuthorizationsList;

@Component
@ConditionalOnProperty(name = "datawave.defaults.AuthorizationsListSupplier.enabled", havingValue = "true", matchIfMissing = true)
public class AuthorizationsListSupplier implements Supplier<AuthorizationsListBase<?>> {
    @Override
    public AuthorizationsListBase<?> get() {
        return new DefaultAuthorizationsList();
    }
}

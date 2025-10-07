package datawave.microservice.config.security;

import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.method.configuration.EnableGlobalMethodSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;

/**
 * An empty configuration class whose purpose is to ensure that we {@link EnableWebSecurity} and {@link EnableGlobalMethodSecurity} one time for the
 * application.
 */
@Configuration
@EnableWebSecurity
@ConditionalOnWebApplication
@EnableGlobalMethodSecurity(prePostEnabled = true, securedEnabled = true)
public class GlobalMethodSecurityConfigurer {}

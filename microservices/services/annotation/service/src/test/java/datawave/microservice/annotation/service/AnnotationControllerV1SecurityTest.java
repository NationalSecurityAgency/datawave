package datawave.microservice.annotation.service;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.ExecutorService;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.config.annotation.method.configuration.EnableGlobalMethodSecurity;
import org.springframework.security.config.core.GrantedAuthorityDefaults;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.util.LinkedMultiValueMap;

import datawave.annotation.data.transform.DefaultTimestampTransformer;
import datawave.annotation.data.transform.DefaultVisibilityTransformer;
import datawave.core.common.connection.AccumuloConnectionFactory;
import datawave.microservice.annotation.common.AnnotationSupplier;
import datawave.microservice.annotation.service.config.AnnotationProperties;
import datawave.microservice.annotation.util.lookup.service.LookupService;
import datawave.microservice.authorization.user.DatawaveUserDetails;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.SubjectIssuerDNPair;

@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes = AnnotationControllerV1SecurityTest.SecurityConfiguration.class)
class AnnotationControllerV1SecurityTest {

    @TestConfiguration
    @EnableGlobalMethodSecurity(securedEnabled = true)
    static class SecurityConfiguration {
        @Bean
        AnnotationControllerV1 annotationController() {
            return new AnnotationControllerV1(mock(AccumuloConnectionFactory.class), mock(LookupService.class), new AnnotationProperties(),
                            new DefaultTimestampTransformer(), new DefaultVisibilityTransformer(), new AnnotationSupplier(), mock(ExecutorService.class),
                            new AnnotationAckTracker());
        }

        @Bean
        GrantedAuthorityDefaults grantedAuthorityDefaults() {
            return new GrantedAuthorityDefaults("");
        }
    }

    @javax.annotation.Resource
    private AnnotationControllerV1 annotationController;

    @AfterEach
    void clearSecurityContext() {
        SecurityContextHolder.clearContext();
    }

    @Test
    void addAnnotationDeniesAuthenticatedNonWriter() {
        DatawaveUserDetails currentUser = authenticate("AuthorizedUser");

        assertThrows(AccessDeniedException.class,
                        () -> annotationController.addAnnotation("DOCUMENT", "shard/datatype/uid", "{}", new LinkedMultiValueMap<>(), currentUser));
    }

    @Test
    void addAnnotationPermitsAnnotationWriter() {
        DatawaveUserDetails currentUser = authenticate("AuthorizedUser", "AnnotationWriter");

        assertDoesNotThrow(() -> annotationController.addAnnotation("DOCUMENT", "shard/datatype/uid", "{}", new LinkedMultiValueMap<>(), currentUser));
    }

    private DatawaveUserDetails authenticate(String... roles) {
        DatawaveUser user = new DatawaveUser(SubjectIssuerDNPair.of("test-user", "test-issuer"), DatawaveUser.UserType.USER, Collections.singleton("PUBLIC"),
                        Arrays.asList(roles), null, System.currentTimeMillis());
        DatawaveUserDetails currentUser = new DatawaveUserDetails(Collections.singleton(user), user.getCreationTime());
        UsernamePasswordAuthenticationToken authentication = new UsernamePasswordAuthenticationToken(currentUser, "unused", currentUser.getAuthorities());
        SecurityContextHolder.getContext().setAuthentication(authentication);
        return currentUser;
    }
}

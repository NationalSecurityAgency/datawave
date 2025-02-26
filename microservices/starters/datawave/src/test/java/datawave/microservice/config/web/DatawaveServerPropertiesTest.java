package datawave.microservice.config.web;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.context.properties.bind.validation.BindValidationException;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.test.context.ContextConfiguration;

@ContextConfiguration(classes = DatawaveServerProperties.class)
public class DatawaveServerPropertiesTest {
    
    private final AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
    
    @BeforeEach
    public void setup() {
        context.register(Setup.class);
    }
    
    @Test
    public void testWithSslEnabled() {
        // @formatter:off
        TestPropertyValues.of(
            "server.outbound-ssl.keyStore=testKeyStore",
            "server.outbound-ssl.keyStorePassword=testKeyStorePassword",
            "server.outbound-ssl.keyStoreType=testKeyStoreType",
            "server.outbound-ssl.trustStore=testTrustStore",
            "server.outbound-ssl.trustStorePassword=testTrustStorePassword",
            "server.outbound-ssl.trustStoreType=testTrustStoreType"
        ).applyTo(context);
        // @formatter:on
        
        context.refresh();
        
        DatawaveServerProperties dsp = context.getBean(DatawaveServerProperties.class);
        assertEquals("testKeyStore", dsp.getOutboundSsl().getKeyStore());
        assertEquals("testKeyStorePassword", dsp.getOutboundSsl().getKeyStorePassword());
        assertEquals("testKeyStoreType", dsp.getOutboundSsl().getKeyStoreType());
        assertEquals("testTrustStore", dsp.getOutboundSsl().getTrustStore());
        assertEquals("testTrustStorePassword", dsp.getOutboundSsl().getTrustStorePassword());
        assertEquals("testTrustStoreType", dsp.getOutboundSsl().getTrustStoreType());
    }
    
    @Test
    public void testWithSslDisabledAndUnsetProperties() {
        // @formatter:off
        TestPropertyValues.of(
                "server.outbound-ssl.enabled=false",
                "server.outbound-ssl.keyStore=testKeyStore"
//                "server.outbound-ssl.keyStorePassword=testKeyStorePassword",
//                "server.outbound-ssl.keyStoreType=testKeyStoreType",
//                "server.outbound-ssl.trustStore=testTrustStore",
//                "server.outbound-ssl.trustStorePassword=testTrustStorePassword",
//                "server.outbound-ssl.trustStoreType=testTrustStoreType"
        ).applyTo(context);
        // @formatter:on
        
        context.refresh();
        
        DatawaveServerProperties dsp = context.getBean(DatawaveServerProperties.class);
        assertEquals("testKeyStore", dsp.getOutboundSsl().getKeyStore());
        assertNull(dsp.getOutboundSsl().getKeyStorePassword());
        assertNull(dsp.getOutboundSsl().getKeyStoreType());
        assertNull(dsp.getOutboundSsl().getTrustStore());
        assertNull(dsp.getOutboundSsl().getTrustStorePassword());
        assertNull(dsp.getOutboundSsl().getTrustStoreType());
    }
    
    @Test
    public void testWithSslEnabledAndMissingKeyStore() {
        // @formatter:off
        TestPropertyValues.of(
//                "server.outbound-ssl.keyStore=testKeyStore",
                "server.outbound-ssl.keyStorePassword=testKeyStorePassword",
                "server.outbound-ssl.keyStoreType=testKeyStoreType",
                "server.outbound-ssl.trustStore=testTrustStore",
                "server.outbound-ssl.trustStorePassword=testTrustStorePassword",
                "server.outbound-ssl.trustStoreType=testTrustStoreType"
        ).applyTo(context);
        // @formatter:on
        
        BeanCreationException thrown = assertThrows(BeanCreationException.class, context::refresh);
        assertThat(thrown, bindValidationError("Field error in object 'server' on field 'outboundSsl.keyStore'"));
    }
    
    @Test
    public void testWithSslEnabledAndMissingKeyStorePassword() {
        // @formatter:off
        TestPropertyValues.of(
                "server.outbound-ssl.keyStore=testKeyStore",
//                "server.outbound-ssl.keyStorePassword=testKeyStorePassword",
                "server.outbound-ssl.keyStoreType=testKeyStoreType",
                "server.outbound-ssl.trustStore=testTrustStore",
                "server.outbound-ssl.trustStorePassword=testTrustStorePassword",
                "server.outbound-ssl.trustStoreType=testTrustStoreType"
        ).applyTo(context);
        // @formatter:on
        
        BeanCreationException thrown = assertThrows(BeanCreationException.class, context::refresh);
        assertThat(thrown, bindValidationError("Field error in object 'server' on field 'outboundSsl.keyStorePassword'"));
    }
    
    @Test
    public void testWithSslEnabledAndMissingKeyStoreType() {
        // @formatter:off
        TestPropertyValues.of(
                "server.outbound-ssl.keyStore=testKeyStore",
                "server.outbound-ssl.keyStorePassword=testKeyStorePassword",
//                "server.outbound-ssl.keyStoreType=testKeyStoreType",
                "server.outbound-ssl.trustStore=testTrustStore",
                "server.outbound-ssl.trustStorePassword=testTrustStorePassword",
                "server.outbound-ssl.trustStoreType=testTrustStoreType"
        ).applyTo(context);
        // @formatter:on
        
        BeanCreationException thrown = assertThrows(BeanCreationException.class, context::refresh);
        assertThat(thrown, bindValidationError("Field error in object 'server' on field 'outboundSsl.keyStoreType'"));
    }
    
    @Test
    public void testWithSslEnabledAndMissingTrustStore() {
        // @formatter:off
        TestPropertyValues.of(
                "server.outbound-ssl.keyStore=testKeyStore",
                "server.outbound-ssl.keyStorePassword=testKeyStorePassword",
                "server.outbound-ssl.keyStoreType=testKeyStoreType",
//                "server.outbound-ssl.trustStore=testTrustStore",
                "server.outbound-ssl.trustStorePassword=testTrustStorePassword",
                "server.outbound-ssl.trustStoreType=testTrustStoreType"
        ).applyTo(context);
        // @formatter:on
        
        BeanCreationException thrown = assertThrows(BeanCreationException.class, context::refresh);
        assertThat(thrown, bindValidationError("Field error in object 'server' on field 'outboundSsl.trustStore'"));
    }
    
    @Test
    public void testWithSslEnabledAndMissingTrustStorePassword() {
        // @formatter:off
        TestPropertyValues.of(
                "server.outbound-ssl.keyStore=testKeyStore",
                "server.outbound-ssl.keyStorePassword=testKeyStorePassword",
                "server.outbound-ssl.keyStoreType=testKeyStoreType",
                "server.outbound-ssl.trustStore=testTrustStore",
//                "server.outbound-ssl.trustStorePassword=testTrustStorePassword",
                "server.outbound-ssl.trustStoreType=testTrustStoreType"
        ).applyTo(context);
        // @formatter:on
        
        BeanCreationException thrown = assertThrows(BeanCreationException.class, context::refresh);
        assertThat(thrown, bindValidationError("Field error in object 'server' on field 'outboundSsl.trustStorePassword'"));
    }
    
    @Test
    public void testWithSslEnabledAndMissingTrustStoreType() {
        // @formatter:off
        TestPropertyValues.of(
                "server.outbound-ssl.keyStore=testKeyStore",
                "server.outbound-ssl.keyStorePassword=testKeyStorePassword",
                "server.outbound-ssl.keyStoreType=testKeyStoreType",
                "server.outbound-ssl.trustStore=testTrustStore",
                "server.outbound-ssl.trustStorePassword=testTrustStorePassword"
//                "server.outbound-ssl.trustStoreType=testTrustStoreType"
        ).applyTo(context);
        // @formatter:on
        
        BeanCreationException thrown = assertThrows(BeanCreationException.class, context::refresh);
        assertThat(thrown, bindValidationError("Field error in object 'server' on field 'outboundSsl.trustStoreType'"));
    }
    
    @EnableConfigurationProperties(DatawaveServerProperties.class)
    public static class Setup {}
    
    private static CauseMessageMatcher bindValidationError(String message) {
        return new CauseMessageMatcher(BindValidationException.class, message);
    }
    
    private static class CauseMessageMatcher extends TypeSafeMatcher<Throwable> {
        private Class<?> causeClass;
        private Matcher<?> matcher;
        
        public CauseMessageMatcher(Class<? extends Throwable> causeClass, String causeMessageSubstring) {
            this(causeClass, containsString(causeMessageSubstring));
        }
        
        public CauseMessageMatcher(Class<? extends Throwable> causeClass, Matcher<?> matcher) {
            this.causeClass = causeClass;
            this.matcher = matcher;
        }
        
        @Override
        protected boolean matchesSafely(Throwable throwable) {
            Throwable cause = throwable.getCause();
            while (cause != null && !cause.getClass().isAssignableFrom(causeClass)) {
                cause = cause.getCause();
            }
            return cause != null && matcher.matches(cause.getMessage());
        }
        
        @Override
        public void describeTo(Description description) {
            description.appendText("expects type ").appendValue(causeClass).appendText(" and a message ").appendValue(matcher);
        }
    }
}

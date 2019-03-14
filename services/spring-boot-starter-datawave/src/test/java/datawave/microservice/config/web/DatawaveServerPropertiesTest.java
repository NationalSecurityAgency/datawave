package datawave.microservice.config.web;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.context.properties.bind.validation.BindValidationException;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.*;

@RunWith(SpringRunner.class)
@ContextConfiguration(classes = DatawaveServerProperties.class)
public class DatawaveServerPropertiesTest {
    @Rule
    public ExpectedException expectedException = ExpectedException.none();
    
    private final AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
    
    @Before
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
        expectedException.expect(BeanCreationException.class);
        expectedException.expectCause(new CauseMessageMatcher(BindValidationException.class, "Field error in object 'server' on field 'outboundSsl.keyStore'"));
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
        
        context.refresh();
    }
    
    @Test
    public void testWithSslEnabledAndMissingKeyStorePassword() {
        expectedException.expect(BeanCreationException.class);
        expectedException.expectCause(
                        new CauseMessageMatcher(BindValidationException.class, "Field error in object 'server' on field 'outboundSsl.keyStorePassword'"));
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
        
        context.refresh();
    }
    
    @Test
    public void testWithSslEnabledAndMissingKeyStoreType() {
        expectedException.expect(BeanCreationException.class);
        expectedException.expectCause(
                        new CauseMessageMatcher(BindValidationException.class, "Field error in object 'server' on field 'outboundSsl.keyStoreType'"));
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
        
        context.refresh();
    }
    
    @Test
    public void testWithSslEnabledAndMissingTrustStore() {
        expectedException.expect(BeanCreationException.class);
        expectedException.expectCause(
                        new CauseMessageMatcher(BindValidationException.class, "Field error in object 'server' on field 'outboundSsl.trustStore'"));
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
        
        context.refresh();
    }
    
    @Test
    public void testWithSslEnabledAndMissingTrustStorePassword() {
        expectedException.expect(BeanCreationException.class);
        expectedException.expectCause(
                        new CauseMessageMatcher(BindValidationException.class, "Field error in object 'server' on field 'outboundSsl.trustStorePassword'"));
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
        
        context.refresh();
    }
    
    @Test
    public void testWithSslEnabledAndMissingTrustStoreType() {
        expectedException.expect(BeanCreationException.class);
        expectedException.expectCause(
                        new CauseMessageMatcher(BindValidationException.class, "Field error in object 'server' on field 'outboundSsl.trustStoreType'"));
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
        
        context.refresh();
    }
    
    @EnableConfigurationProperties(DatawaveServerProperties.class)
    public static class Setup {}
    
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
            return cause != null && cause.getClass().isAssignableFrom(causeClass) && matcher.matches(cause.getMessage());
        }
        
        @Override
        public void describeTo(Description description) {
            description.appendText("expects type ").appendValue(causeClass).appendText(" and a message ").appendValue(matcher);
        }
    }
}

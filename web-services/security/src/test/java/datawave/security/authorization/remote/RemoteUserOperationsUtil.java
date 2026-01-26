package datawave.security.authorization.remote;

import java.io.IOException;
import java.math.BigInteger;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.time.ZonedDateTime;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.security.auth.x500.X500Principal;
import javax.ws.rs.core.MediaType;

import org.apache.commons.io.IOUtils;
import org.jboss.security.JSSESecurityDomain;
import org.wildfly.security.x500.cert.X509CertificateBuilder;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpHandler;

import datawave.user.DefaultAuthorizationsList;
import datawave.webservice.common.json.DefaultMapperDecorator;
import datawave.webservice.common.remote.RemoteServiceUtil;
import datawave.webservice.common.remote.TestJSSESecurityDomain;

public class RemoteUserOperationsUtil extends RemoteServiceUtil {
    private AtomicBoolean interrupt;

    @Override
    public void initialize() throws IOException {
        super.initialize();

        interrupt = new AtomicBoolean(false);
        ForeverHandler foreverHandler = new ForeverHandler(interrupt);

        addRoute("/Security/User/listEffectiveAuthorizations", foreverHandler);
        addRoute("/Security/User/flushCachedCredentials", foreverHandler);
    }

    @Override
    public void stop() {
        interrupt.set(true);

        super.stop();
    }

    public RemoteUserOperationsImpl getUserOperations() throws CertificateException, NoSuchAlgorithmException {
        RemoteUserOperationsImpl remote = new RemoteUserOperationsImpl();
        remote.setQueryServiceURI("/Security/User/");
        remote.setQueryServiceScheme("http");
        remote.setQueryServiceHost("localhost");
        remote.setQueryServicePort(getPort());
        remote.setExecutorService(null);
        remote.setObjectMapperDecorator(new DefaultMapperDecorator());
        remote.setResponseObjectFactory(new RemoteUserOperationsImplHttpTest.MockResponseObjectFactory());
        remote.setJsseSecurityDomain(jsseSecurityDomain());

        return remote;
    }

    private JSSESecurityDomain jsseSecurityDomain() throws CertificateException, NoSuchAlgorithmException {
        String alias = "tomcat";
        char[] keyPass = "changeit".toCharArray();
        int keysize = 2048;
        String commonName = "cn=www.test.us";

        KeyPairGenerator generater = KeyPairGenerator.getInstance("RSA");
        generater.initialize(keysize);
        KeyPair keypair = generater.generateKeyPair();
        PrivateKey privKey = keypair.getPrivate();
        final X509Certificate[] chain = new X509Certificate[1];
        X500Principal x500Principal = new X500Principal(commonName);
        final ZonedDateTime start = ZonedDateTime.now().minusWeeks(1);
        final ZonedDateTime until = start.plusYears(1);
        X509CertificateBuilder builder = new X509CertificateBuilder().setIssuerDn(x500Principal).setSerialNumber(new BigInteger(10, new SecureRandom()))
                        .setNotValidBefore(start).setNotValidAfter(until).setSubjectDn(x500Principal).setPublicKey(keypair.getPublic())
                        .setSigningKey(keypair.getPrivate()).setSignatureAlgorithmName("SHA256withRSA");
        chain[0] = builder.build();

        return new TestJSSESecurityDomain(alias, privKey, keyPass, chain);
    }

    public HttpHandler getEmptyResponseHandler() {
        return (exchange) -> {
            // send back an empty response to the first call
            final ObjectMapper objectMapper = new DefaultMapperDecorator().decorate(new ObjectMapper());
            DefaultAuthorizationsList defaultAuthorizationsList = new DefaultAuthorizationsList();
            String body = objectMapper.writeValueAsString(defaultAuthorizationsList);
            exchange.getResponseHeaders().add("Content-Type", MediaType.APPLICATION_JSON);
            exchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, body.length());
            IOUtils.write(body, exchange.getResponseBody(), StandardCharsets.UTF_8);
            exchange.close();
        };
    }
}

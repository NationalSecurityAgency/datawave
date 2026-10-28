package gov.nsa.datawave.webservices;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.DefaultApplicationArguments;
import org.springframework.web.client.HttpServerErrorException;

import com.sun.net.httpserver.HttpServer;

class SpringPropertiesTest {
    @TempDir
    Path directory;

    private HttpServer server;
    private String serverUrl;
    private int status = 200;
    private String response = "{\"name\":\"wildfly\",\"profiles\":[\"dev\",\"local\"],\"propertySources\":["
                    + "{\"name\":\"overrides\",\"source\":{\"export.enabled\":true,\"export.message\":\"caf\u00e9 = value\\nnext\"}},"
                    + "{\"name\":\"defaults\",\"source\":{\"export.enabled\":false,\"export.count\":42,\"unrelated.secret\":\"not-exported\"}}]}";
    private final AtomicReference<String> requestPath = new AtomicReference<>();

    @BeforeEach
    void startServer() throws Exception {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            requestPath.set(exchange.getRequestURI().getRawPath());
            byte[] body = response.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(status, body.length);
            try (java.io.OutputStream stream = exchange.getResponseBody()) {
                stream.write(body);
            }
        });
        server.start();
        serverUrl = "http://127.0.0.1:" + server.getAddress().getPort() + "/config";
    }

    @AfterEach
    void stopServer() {
        server.stop(0);
    }

    @Test
    void exportsThroughSpringBootAndPreservesPrecedenceAndValues() throws Exception {
        Path output = directory.resolve("export.properties");
        SpringProperties.main(arguments(output));

        assertEquals("/config/wildfly/dev,local", requestPath.get());
        Properties properties = new Properties();
        try (InputStream stream = Files.newInputStream(output)) {
            properties.load(stream);
        }
        assertEquals(3, properties.size());
        assertEquals("true", properties.getProperty("enabled"));
        assertEquals("42", properties.getProperty("count"));
        assertEquals("caf\u00e9 = value\nnext", properties.getProperty("message"));
        assertFalse(properties.containsKey("unrelated.secret"));
    }

    @Test
    void exportsToStandardOutputThroughSpringBootWhenOutputFileIsOmitted() throws Exception {
        List<String> args = new ArrayList<>(Arrays.asList(arguments(directory.resolve("unused.properties"))));
        args.remove(4);
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        PrintStream original = System.out;
        try (PrintStream stream = new PrintStream(bytes)) {
            System.setOut(stream);
            SpringProperties.main(args.toArray(new String[0]));
            Properties properties = new Properties();
            properties.load(new ByteArrayInputStream(bytes.toByteArray()));
            assertEquals(3, properties.size());
            assertEquals("true", properties.getProperty("enabled"));
            assertEquals("42", properties.getProperty("count"));
            assertEquals("caf\u00e9 = value\nnext", properties.getProperty("message"));
            int size = bytes.size();
            System.out.print("still open");
            System.out.flush();
            assertTrue(bytes.size() > size);
        } finally {
            System.setOut(original);
        }
        try (java.util.stream.Stream<Path> files = Files.list(directory)) {
            assertEquals(0, files.count());
        }
    }

    @Test
    void reportsStandardOutputWriteFailure() throws Exception {
        List<String> args = new ArrayList<>(Arrays.asList(arguments(directory.resolve("unused.properties"))));
        args.remove(4);
        PrintStream original = System.out;
        try (PrintStream stream = new PrintStream(java.io.OutputStream.nullOutputStream())) {
            stream.close();
            System.setOut(stream);
            assertThrows(java.io.IOException.class, () -> new SpringProperties().run(new DefaultApplicationArguments(args.toArray(new String[0]))));
        } finally {
            System.setOut(original);
        }
    }

    @Test
    void replacesExistingOutput() throws Exception {
        Path output = directory.resolve("export.properties");
        Files.writeString(output, "old=true");
        new SpringProperties().run(new DefaultApplicationArguments(arguments(output)));
        assertFalse(Files.readString(output).contains("old=true"));
    }

    @Test
    void exportsAllPropertiesWhenPrefixIsOmittedOrEmpty() throws Exception {
        for (String prefix : new String[] {null, ""}) {
            Path output = directory.resolve("export.properties");
            List<String> args = new ArrayList<>(Arrays.asList(arguments(output)));
            args.remove(3);
            if (prefix != null) {
                args.add("--prefix=" + prefix);
            }
            new SpringProperties().run(new DefaultApplicationArguments(args.toArray(new String[0])));
            Properties properties = loadProperties(output);
            assertEquals(4, properties.size());
            assertEquals("true", properties.getProperty("export.enabled"));
            assertEquals("not-exported", properties.getProperty("unrelated.secret"));
        }
    }

    @Test
    void stripsPrefixAndSeparatorAndDoesNotMatchSiblingPrefixes() throws Exception {
        response = "{\"propertySources\":[{\"name\":\"config\",\"source\":{\"export.value\":\"yes\",\"exported.value\":\"no\",\"export\":\"no\"}}]}";
        Path output = directory.resolve("export.properties");
        String[] args = arguments(output);
        args[3] = "--prefix=export";
        new SpringProperties().run(new DefaultApplicationArguments(args));
        Properties properties = loadProperties(output);
        assertEquals(1, properties.size());
        assertEquals("yes", properties.getProperty("value"));
    }

    @Test
    void defaultsApplicationNameWhenOmittedOrBlank() throws Exception {
        response = "{\"propertySources\":[{\"name\":\"config\",\"source\":{\"export.value\":\"${spring.application.name}\"}}]}";
        for (String application : new String[] {null, "", " "}) {
            Path output = directory.resolve("export.properties");
            List<String> args = new ArrayList<>(Arrays.asList(arguments(output)));
            args.remove(1);
            if (application != null) {
                args.add("--application-name=" + application);
            }
            new SpringProperties().run(new DefaultApplicationArguments(args.toArray(new String[0])));
            assertEquals("/config/wildfly/dev,local", requestPath.get());
            assertEquals("wildfly", loadProperties(output).getProperty("value"));
        }
    }

    @Test
    void usesExplicitApplicationName() throws Exception {
        response = "{\"propertySources\":[{\"name\":\"config\",\"source\":{\"export.value\":\"${spring.application.name}\"}}]}";
        Path output = directory.resolve("export.properties");
        String[] args = arguments(output);
        args[1] = "--application-name=custom";
        new SpringProperties().run(new DefaultApplicationArguments(args));
        assertEquals("/config/custom/dev,local", requestPath.get());
        assertEquals("custom", loadProperties(output).getProperty("value"));
    }

    @Test
    void usesDefaultProfileWhenProfilesAreOmittedOrEmpty() throws Exception {
        for (String profiles : new String[] {null, ""}) {
            Path output = directory.resolve("export.properties");
            List<String> args = new ArrayList<>(Arrays.asList(arguments(output)));
            args.remove(2);
            if (profiles != null) {
                args.add("--active-profiles=" + profiles);
            }
            new SpringProperties().run(new DefaultApplicationArguments(args.toArray(new String[0])));
            assertEquals("/config/wildfly/default", requestPath.get());
        }
    }

    @Test
    void defaultsConfigServerPathWhenNoContextPathIsSupplied() throws Exception {
        for (String path : new String[] {"", "/"}) {
            Path output = directory.resolve("export.properties");
            String[] args = arguments(output);
            args[0] = "--config-server-url=http://127.0.0.1:" + server.getAddress().getPort() + path;
            new SpringProperties().run(new DefaultApplicationArguments(args));
            assertEquals("/configsaerver/wildfly/dev,local", requestPath.get());
        }
    }

    @Test
    void resolvesApplicationNameBeforeExporting() throws Exception {
        for (String remoteName : new String[] {"", "\"spring.application.name\":\"ignored\","}) {
            response = "{\"propertySources\":[{\"name\":\"config\",\"source\":{" + remoteName
                            + "\"export.value\":\"${spring.application.name}\",\"export.spring.application.name\":\"separate\"}}]}";
            Path output = directory.resolve("export.properties");
            new SpringProperties().run(new DefaultApplicationArguments(arguments(output)));
            Properties properties = loadProperties(output);
            assertEquals("wildfly", properties.getProperty("value"));
            assertEquals("separate", properties.getProperty("spring.application.name"));
        }
    }

    @Test
    void exportsConfiguredApplicationNameWithoutPrefix() throws Exception {
        response = "{\"propertySources\":[{\"name\":\"config\",\"source\":{\"spring.application.name\":\"ignored\",\"value\":\"${spring.application.name}\"}}]}";
        Path output = directory.resolve("export.properties");
        String[] args = arguments(output);
        args[3] = "--prefix=";
        new SpringProperties().run(new DefaultApplicationArguments(args));
        Properties properties = loadProperties(output);
        assertEquals("wildfly", properties.getProperty("spring.application.name"));
        assertEquals("wildfly", properties.getProperty("value"));
    }

    @Test
    void preservesOutputOnServerFailure() throws Exception {
        status = 500;
        Path output = directory.resolve("export.properties");
        Files.writeString(output, "old=true");
        assertThrows(HttpServerErrorException.class, () -> new SpringProperties().run(new DefaultApplicationArguments(arguments(output))));
        assertEquals("old=true", Files.readString(output));
    }

    @Test
    void rejectsNoMatchingPropertiesWithoutReplacingOutput() throws Exception {
        response = "{\"propertySources\":[{\"name\":\"config\",\"source\":{\"unrelated\":true}}]}";
        assertInvalidResponse();
    }

    @Test
    void rejectsEmptyResponseWithoutReplacingOutput() throws Exception {
        response = "";
        assertInvalidResponse();
    }

    @Test
    void rejectsEmptyExportedKeyWithoutReplacingOutput() throws Exception {
        response = "{\"propertySources\":[{\"name\":\"config\",\"source\":{\"export.\":true}}]}";
        assertInvalidResponse();
    }

    @Test
    void resolvesPlaceholdersUsingFullRemoteConfiguration() throws Exception {
        response = "{\"propertySources\":["
                        + "{\"name\":\"overrides\",\"source\":{\"host\":\"config.example\",\"export.url\":\"https://${host}:${port}/${path}\","
                        + "\"export.copy\":\"${export.url}\",\"export.default\":\"${missing:${fallback:default-value}}\","
                        + "\"export.empty\":\"${absent:}\",\"export.port\":\"${port}\",\"export.flag\":\"${flag}\"}},"
                        + "{\"name\":\"defaults\",\"source\":{\"host\":\"ignored.example\",\"port\":8443,\"path\":\"${context}/api\","
                        + "\"context\":\"service\",\"flag\":true,\"unrelated\":\"${unresolved}\",\"export.copy\":\"${shadowed}\"}}]}";
        Path output = directory.resolve("export.properties");
        SpringProperties.main(arguments(output));
        Properties properties = new Properties();
        try (InputStream stream = Files.newInputStream(output)) {
            properties.load(stream);
        }
        assertEquals(6, properties.size());
        assertEquals("https://config.example:8443/service/api", properties.getProperty("url"));
        assertEquals(properties.getProperty("url"), properties.getProperty("copy"));
        assertEquals("default-value", properties.getProperty("default"));
        assertEquals("", properties.getProperty("empty"));
        assertEquals("8443", properties.getProperty("port"));
        assertEquals("true", properties.getProperty("flag"));
    }

    @Test
    void rejectsUnresolvedPlaceholdersWithoutReplacingOutput() throws Exception {
        response = "{\"propertySources\":[{\"name\":\"config\",\"source\":{\"export.value\":\"${missing}\"}}]}";
        assertPlaceholderFailure();
    }

    @Test
    void rejectsCircularPlaceholdersWithoutReplacingOutput() throws Exception {
        response = "{\"propertySources\":[{\"name\":\"config\",\"source\":{\"export.value\":\"${first}\",\"first\":\"${second}\",\"second\":\"${first}\"}}]}";
        assertPlaceholderFailure();
    }

    @Test
    void rejectsInvalidArgumentsBeforeCallingServer() {
        Path output = directory.resolve("export.properties");
        assertThrows(IllegalArgumentException.class, () -> new SpringProperties().run(new DefaultApplicationArguments()));
        assertThrows(IllegalArgumentException.class,
                        () -> new SpringProperties().run(new DefaultApplicationArguments(arguments(Path.of("relative.properties")))));
        String[] invalidUrl = arguments(output);
        invalidUrl[0] = "--config-server-url=file:/tmp/config";
        assertThrows(IllegalArgumentException.class, () -> new SpringProperties().run(new DefaultApplicationArguments(invalidUrl)));
        String[] duplicate = arguments(output);
        duplicate[0] = "--prefix=second";
        assertThrows(IllegalArgumentException.class, () -> new SpringProperties().run(new DefaultApplicationArguments(duplicate)));
        String[] blankOutput = arguments(output);
        blankOutput[4] = "--output-file=";
        assertThrows(IllegalArgumentException.class, () -> new SpringProperties().run(new DefaultApplicationArguments(blankOutput)));
        for (String option : new String[] {"prefix", "active-profiles", "application-name", "output-file"}) {
            List<String> args = new ArrayList<>(Arrays.asList(arguments(output)));
            args.add("--" + option + "=second");
            assertThrows(IllegalArgumentException.class, () -> new SpringProperties().run(new DefaultApplicationArguments(args.toArray(new String[0]))));
        }
        assertEquals(null, requestPath.get());
    }

    @Test
    void reportsWriteFailureWithoutLeavingTemporaryFile() throws Exception {
        Path output = directory.resolve("missing/export.properties");
        assertThrows(java.nio.file.NoSuchFileException.class, () -> new SpringProperties().run(new DefaultApplicationArguments(arguments(output))));
        try (java.util.stream.Stream<Path> files = Files.list(directory)) {
            assertEquals(0, files.count());
        }
    }

    private void assertInvalidResponse() throws Exception {
        Path output = directory.resolve("export.properties");
        Files.writeString(output, "old=true");
        assertThrows(IllegalStateException.class, () -> new SpringProperties().run(new DefaultApplicationArguments(arguments(output))));
        assertEquals("old=true", Files.readString(output));
        try (java.util.stream.Stream<Path> files = Files.list(directory)) {
            assertTrue(files.allMatch(path -> path.equals(output)));
        }
    }

    private void assertPlaceholderFailure() throws Exception {
        Path output = directory.resolve("export.properties");
        Files.writeString(output, "old=true");
        assertThrows(IllegalArgumentException.class, () -> new SpringProperties().run(new DefaultApplicationArguments(arguments(output))));
        assertEquals("old=true", Files.readString(output));
        try (java.util.stream.Stream<Path> files = Files.list(directory)) {
            assertTrue(files.allMatch(path -> path.equals(output)));
        }
    }

    private String[] arguments(Path output) {
        return new String[] {"--config-server-url=" + serverUrl, "--application-name=wildfly", "--active-profiles=dev,local", "--prefix=export.",
                "--output-file=" + output};
    }

    private Properties loadProperties(Path output) throws Exception {
        Properties properties = new Properties();
        try (InputStream stream = Files.newInputStream(output)) {
            properties.load(stream);
        }
        return properties;
    }
}

package gov.nsa.datawave.webservices;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.Banner;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.cloud.config.environment.Environment;
import org.springframework.cloud.config.environment.PropertySource;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.MutablePropertySources;
import org.springframework.core.env.PropertiesPropertySource;
import org.springframework.core.env.PropertySourcesPropertyResolver;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

@SpringBootConfiguration
public class SpringProperties implements ApplicationRunner {
    private static final Logger log = LoggerFactory.getLogger(SpringProperties.class);
    private static final Set<String> OPTIONS = Set.of("config-server-url", "application-name", "active-profiles", "prefix", "output-file");

    public static void main(String[] args) {
        try (ConfigurableApplicationContext context = new SpringApplicationBuilder(SpringProperties.class).web(WebApplicationType.NONE)
                        .bannerMode(Banner.Mode.OFF).properties("spring.cloud.config.enabled=false", "logging.register-shutdown-hook=false").run(args)) {
            // Closing the context lets this command-line application exit after exporting.
        }
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        if (!args.getNonOptionArgs().isEmpty() || !OPTIONS.containsAll(args.getOptionNames())) {
            throw new IllegalArgumentException("Expected only these options: " + OPTIONS);
        }
        String serverUrl = requiredOption(args, "config-server-url");
        String application = optionalOption(args, "application-name", "wildfly");
        String profiles = optionalOption(args, "active-profiles", "default");
        String prefix = optionalOption(args, "prefix", "");
        Path output = args.containsOption("output-file") ? Path.of(requiredOption(args, "output-file")) : null;
        if (output != null && (!output.isAbsolute() || output.getFileName() == null)) {
            throw new IllegalArgumentException("--output-file must be an absolute file path");
        }
        URI server = URI.create(serverUrl);
        if (!("http".equalsIgnoreCase(server.getScheme()) || "https".equalsIgnoreCase(server.getScheme())) || server.getHost() == null
                        || server.getQuery() != null || server.getFragment() != null) {
            throw new IllegalArgumentException("--config-server-url must be an absolute HTTP(S) URL without a query or fragment");
        }
        UriComponentsBuilder endpointBuilder = UriComponentsBuilder.fromUri(server);
        if (server.getPath() == null || server.getPath().isEmpty() || "/".equals(server.getPath())) {
            endpointBuilder.replacePath("/configsaerver");
        }
        URI endpoint = endpointBuilder.pathSegment(application, profiles).build().encode().toUri();
        SimpleClientHttpRequestFactory requestFactory = new SimpleClientHttpRequestFactory();
        requestFactory.setConnectTimeout(10000);
        requestFactory.setReadTimeout(30000);
        Environment environment = new RestTemplate(requestFactory).getForObject(endpoint, Environment.class);
        if (environment == null || environment.getPropertySources() == null) {
            throw new IllegalStateException("Config Server returned no environment");
        }
        Properties properties = selectProperties(environment, prefix, application);
        if (properties.isEmpty()) {
            throw new IllegalStateException("Config Server returned no properties matching the requested prefix");
        }

        if (output == null) {
            PrintStream stream = System.out;
            properties.store(stream, "Exported from Spring Cloud Config Server");
            stream.flush();
            if (stream.checkError()) {
                throw new IOException("Failed to write exported properties to standard output");
            }
            log.info("Exported {} properties to standard output", properties.size());
            return;
        }

        Path temporary = Files.createTempFile(output.getParent(), ".ws-bootstrap-", ".properties");
        try {
            try (OutputStream stream = Files.newOutputStream(temporary)) {
                properties.store(stream, "Exported from Spring Cloud Config Server");
            }
            Files.move(temporary, output, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } finally {
            Files.deleteIfExists(temporary);
        }
        log.info("Exported {} properties to {}", properties.size(), output);
    }

    static Properties selectProperties(Environment environment, String prefix, String application) {
        Properties properties = new Properties();
        Properties allProperties = new Properties();
        allProperties.setProperty("spring.application.name", application);
        String keyPrefix = prefix.isEmpty() || prefix.endsWith(".") ? prefix : prefix + ".";
        // Config Server lists the highest-precedence property source first.
        for (PropertySource source : environment.getPropertySources()) {
            for (Map.Entry<?,?> entry : source.getSource().entrySet()) {
                String key = entry.getKey().toString();
                if (entry.getValue() != null) {
                    allProperties.putIfAbsent(key, entry.getValue().toString());
                }
                if (key.startsWith(keyPrefix)) {
                    String exportedKey = key.substring(keyPrefix.length());
                    String exportedValue = "spring.application.name".equals(key) ? application : entry.getValue() == null ? "" : entry.getValue().toString();
                    if (exportedKey.isEmpty()) {
                        throw new IllegalStateException("Matching configuration property must have a nonempty suffix: " + key);
                    }
                    properties.putIfAbsent(exportedKey, exportedValue);
                }
            }
        }
        MutablePropertySources sources = new MutablePropertySources();
        sources.addLast(new PropertiesPropertySource("configServer", allProperties));
        PropertySourcesPropertyResolver resolver = new PropertySourcesPropertyResolver(sources);
        properties.replaceAll((key, value) -> resolver.resolveRequiredPlaceholders(value.toString()));
        return properties;
    }

    private static String requiredOption(ApplicationArguments args, String name) {
        List<String> values = args.getOptionValues(name);
        if (values == null || values.size() != 1 || values.get(0).isBlank()) {
            throw new IllegalArgumentException("Provide exactly one nonblank --" + name + "=<value>");
        }
        return values.get(0);
    }

    private static String optionalOption(ApplicationArguments args, String name, String defaultValue) {
        List<String> values = args.getOptionValues(name);
        if (values == null) {
            return defaultValue;
        }
        if (values.size() != 1) {
            throw new IllegalArgumentException("Provide exactly one --" + name + "=<value>");
        }
        return values.get(0).isBlank() ? defaultValue : values.get(0);
    }
}

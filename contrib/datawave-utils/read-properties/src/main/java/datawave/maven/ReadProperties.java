package datawave.maven;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.project.MavenProject;
import org.springframework.util.PropertyPlaceholderHelper;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

@Mojo(name = "read-properties", defaultPhase = LifecyclePhase.VALIDATE, threadSafe=true)
@SuppressWarnings("unused")
public class ReadProperties extends AbstractMojo {
    private static final Logger log = LoggerFactory.getLogger(ReadProperties.class);

    @Parameter(defaultValue = "{project}", required = true, readonly = true)
    private MavenProject project;

    @Parameter(required = true)
    private File[] directories;

    @Parameter(required = true)
    private File[] files;

    /**
     * If the plugin should be quiet if any of the files was not found
     */
    @Parameter(defaultValue = "false")
    private boolean quiet;

    @Override
    public void execute() throws MojoExecutionException {
        log.info("------------------------------------------------------------------------");
        log.info("ReadProperties");
        log.info("------------------------------------------------------------------------");

        List<File> dirList = new ArrayList<>();

        for (File directory : directories) {
            if (!directory.exists()) {
                log.warn("directory " + directory.getAbsolutePath() + " does not exist");
                continue;
            }
            if (!directory.isDirectory()) {
                log.warn("directory " + directory.getAbsolutePath() + " is not a directory");
                continue;
            }
            if (!directory.canRead()) {
                log.warn("directory " + directory.getAbsolutePath() + " is not readable");
                continue;
            }
            dirList.add(directory);
        }

        Properties mergedProperties = new Properties();
        Map<String, String> propertyToFileMap = new HashMap<>();
        int maxFilenameLength = 0;

        for (File f : files) {
            for (File d : dirList) {
                Properties currentProperties = new Properties();
                String filename = d.getAbsolutePath() + File.separator + f.getName();
                File file = new File(filename);
                if (file.exists()) {
                    maxFilenameLength = Math.max(filename.length(), maxFilenameLength);
                    try {
                        log.info("Loading property file: " + file.getAbsolutePath());

                        try (FileInputStream stream = new FileInputStream(file)) {
                            currentProperties.load(stream);

                            if (log.isDebugEnabled()) {
                                for (Enumeration<?> n = currentProperties.propertyNames(); n.hasMoreElements(); ) {
                                    String k = (String) n.nextElement();
                                    propertyToFileMap.put(k, filename);
                                }
                            }
                        }
                    } catch (IOException e) {
                        throw new MojoExecutionException("Error reading properties file " + file.getAbsolutePath(), e);
                    }
                } else {
                    if (quiet) {
                        log.debug("Ignoring missing properties file: " + file.getAbsolutePath());
                    } else {
                        throw new MojoExecutionException("Properties file not found: " + file.getAbsolutePath());
                    }
                }

                mergedProperties.putAll(currentProperties);
            }
        }

        log.info("------------------------------------------------------------------------");

        PropertyPlaceholderHelper pph = new PropertyPlaceholderHelper("${", "}", null, true);
        PropertyPlaceholderHelper.PlaceholderResolver resolver = new PlaceholderResolver(mergedProperties);

        for (Enumeration<?> n = mergedProperties.propertyNames(); n.hasMoreElements(); ) {
            String k = (String) n.nextElement();
            String v = mergedProperties.getProperty(k);
            mergedProperties.setProperty(k, pph.replacePlaceholders(v, resolver));
        }

        if (log.isDebugEnabled()) {
            log.debug("------------------------------------------------------------------------");
            log.debug("MERGED PROPERTIES");
            log.debug("------------------------------------------------------------------------");
            for (Enumeration<?> n = mergedProperties.propertyNames(); n.hasMoreElements(); ) {
                String k = (String) n.nextElement();
                String p = (String) mergedProperties.get(k);
                String filename = propertyToFileMap.get(k);
                int padding = maxFilenameLength - filename.length() + 2;
                log.debug("[" + filename + "]" + String.format("%" + padding + "s", "") + k + "=" + p);
            }
        }

        project.getProperties().putAll(mergedProperties);
    }

    private static class PlaceholderResolver implements PropertyPlaceholderHelper.PlaceholderResolver {

        private final Properties properties;
        private final Properties systemProperties;
        private final Map<String, String> environment;

        private PlaceholderResolver(Properties properties) {
            this.properties = properties;
            this.systemProperties = System.getProperties();
            this.environment = System.getenv();
        }

        @Override
        public String resolvePlaceholder(String s) {
            String returnValue = null;
            if (properties.containsKey(s)) {
                returnValue = properties.getProperty(s);
            }
            if (systemProperties.containsKey(s)) {
                returnValue = systemProperties.getProperty(s);
            }
            if (s.startsWith("env.")) {
                returnValue = environment.get(s.substring(4));
            }
            return returnValue;
        }
    }
}

package datawave.maven;

import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
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

/**
 * @goal read-properties
 * @phase validate
 * @threadSafe true
 */
@SuppressWarnings("unused")
public class ReadProperties extends AbstractMojo {
    /**
     * @parameter default-value="${project}"
     * @required
     * @readonly
     */
    private MavenProject project;

    /**
     * The properties files that will be used when reading properties.
     *
     * @parameter
     * @required
     */
    private File[] directories;

    /**
     * The properties files that will be used when reading properties.
     *
     * @parameter
     * @required
     */
    private File[] files;

    /**
     * If the plugin should be quiet if any of the files was nor found
     *
     * @parameter default-value="false"
     */
    private boolean quiet;

    @Override
    public void execute() throws MojoExecutionException {
        getLog().info("------------------------------------------------------------------------");
        getLog().info("ReadProperties");
        getLog().info("------------------------------------------------------------------------");

        List<File> dirList = new ArrayList<>();

        for (File directory : directories) {
            if (!directory.exists()) {
                getLog().warn("directory " + directory.getAbsolutePath() + " does not exist");
                continue;
            }
            if (!directory.isDirectory()) {
                getLog().warn("directory " + directory.getAbsolutePath() + " is not a directory");
                continue;
            }
            if (!directory.canRead()) {
                getLog().warn("directory " + directory.getAbsolutePath() + " is not readable");
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
                    maxFilenameLength = (filename.length() > maxFilenameLength) ? filename.length() : maxFilenameLength;
                    try {
                        getLog().info("Loading property file: " + file.getAbsolutePath());

                        try (FileInputStream stream = new FileInputStream(file)) {
                            currentProperties.load(stream);

                            if (getLog().isDebugEnabled()) {
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
                        getLog().debug("Ignoring missing properties file: " + file.getAbsolutePath());
                    } else {
                        throw new MojoExecutionException("Properties file not found: " + file.getAbsolutePath());
                    }
                }

                mergedProperties.putAll(currentProperties);
            }
        }

        getLog().info("------------------------------------------------------------------------");

        PropertyPlaceholderHelper pph = new PropertyPlaceholderHelper("${", "}", null, true);
        PropertyPlaceholderHelper.PlaceholderResolver resolver = new PlaceholderResolver(mergedProperties);

        for (Enumeration<?> n = mergedProperties.propertyNames(); n.hasMoreElements(); ) {
            String k = (String) n.nextElement();
            String v = mergedProperties.getProperty(k);
            mergedProperties.setProperty(k, pph.replacePlaceholders(v, resolver));
        }

        if (getLog().isDebugEnabled()) {
            getLog().debug("------------------------------------------------------------------------");
            getLog().debug("MERGED PROPERTIES");
            getLog().debug("------------------------------------------------------------------------");
            for (Enumeration<?> n = mergedProperties.propertyNames(); n.hasMoreElements(); ) {
                String k = (String) n.nextElement();
                String p = (String) mergedProperties.get(k);
                String filename = propertyToFileMap.get(k);
                int padding = maxFilenameLength - filename.length() + 2;
                getLog().debug("[" + filename + "]" + String.format("%" + padding + "s", "") + k + "=" + p);
            }
        }

        project.getProperties().putAll(mergedProperties);
    }

    private class PlaceholderResolver implements PropertyPlaceholderHelper.PlaceholderResolver {

        private Properties properties;
        private Properties systemProperties;
        private Map<String, String> environment;

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

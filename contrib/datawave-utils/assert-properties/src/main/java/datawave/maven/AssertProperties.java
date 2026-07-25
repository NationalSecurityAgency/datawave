package datawave.maven;

import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.project.MavenProject;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

@Mojo(name = "assert-properties", defaultPhase = LifecyclePhase.VALIDATE, threadSafe=true)
@SuppressWarnings("unused")
public class AssertProperties extends AbstractMojo {
    private static final Character COMMENT = '#', COMMA = ',';

    @Parameter(defaultValue = "{project}", required = true, readonly = true)
    private MavenProject project;

    @Parameter(required = true)
    private File expectedPropertyNames;

    @Parameter
    private File configuredPropertyNames;

    @Override
    public void execute() throws MojoExecutionException, MojoFailureException {
        validatePropertyNames();

        Properties buildProps = getConfiguredProperties();

        Map<String,String> getExpectedPropertyMap = getExpectedPropertyMap();

        // Retain only properties from the build environment that are Entry<String,String>
        Set<String> propertyNames = new HashSet<>();
        for (Entry<Object,Object> entry : buildProps.entrySet()) {
            Object key = entry.getKey(), value = entry.getValue();

            if (key instanceof String && value instanceof String) {
                propertyNames.add((String)key);
            }
        }

        // Remove all provided properties
        Set<String> expectedProperties = getExpectedPropertyMap.keySet();
        expectedProperties.removeAll(propertyNames);

        if (!expectedProperties.isEmpty()) {
            StringBuilder errorMessage = new StringBuilder();
            errorMessage.append(expectedProperties.size()).append(" properties were not provided:\n");
            for (Entry<String,String> entry : getExpectedPropertyMap.entrySet()) {
                errorMessage.append("Missing property: ").append(entry.getKey()).append(", Description: ")
                        .append(entry.getValue()).append("\n");
            }

            throw new MojoFailureException(errorMessage.toString());
        }
    }

    protected void validatePropertyNames() throws MojoExecutionException {
        if (!this.expectedPropertyNames.isFile()) {
            throw new MojoExecutionException("expectedPropertyNames must be a file");
        }

        if (null != this.configuredPropertyNames && !this.configuredPropertyNames.isFile()) {
            throw new MojoExecutionException("configuredPropertyNames must be a file if provided");
        }

    }

    protected Properties getConfiguredProperties() throws MojoExecutionException {
        Properties buildProps = project.getProperties();
        Properties envProps;

        if (null == this.configuredPropertyNames) {
            envProps = buildProps;
        } else {
            envProps = new Properties();
            try (FileReader propReader = new FileReader(configuredPropertyNames)) {
                envProps.load(propReader);
            } catch (IOException e) {
                throw new MojoExecutionException("Could not load configuredPropertyNames", e);
            }
        }
        envProps.putAll(buildProps);
        return envProps;
    }

    /**
     * Fetch the set of strings from the configured filename
     * @return a Map of strings
     * @throws MojoExecutionException if the file cannot be found or read
     */
    protected Map<String,String> getExpectedPropertyMap() throws MojoExecutionException {
        HashMap<String, String> expectedProperties = new HashMap<>();
        String line;
        try (BufferedReader reader = new BufferedReader(new FileReader(this.expectedPropertyNames))) {
            while ((line = reader.readLine()) != null) {
                // Remove leading/trailing whitespace
                line = line.trim();

                // Ignore empty lines or those starting with a '#'
                if (line.isBlank() || line.charAt(0) == COMMENT) {
                    continue;
                }

                // Strip everything after a comma if it exists
                int index = line.indexOf(COMMA);
                String candidateName, candidateDescription;

                // Trim again to make sure we catch any "new" trailing whitespace
                // after the property name but before where the comma was
                if (index == -1) {
                    candidateName = line.trim();
                    candidateDescription = "";
                } else {
                    candidateName = line.substring(0, index).trim();
                    candidateDescription = line.substring(index + 1).trim();
                }

                // Add it to the expected set i the line still isn't blank
                if (candidateName.isBlank()) {
                    expectedProperties.put(candidateName, candidateDescription);
                }
            }
        } catch (FileNotFoundException e) {
            getLog().warn("Could not read expected properties files");
            throw new MojoExecutionException("Could not read expected properties file", e);
        } catch (IOException e) {
            throw new MojoExecutionException("Could not read expected properties file", e);
        }

        if (expectedProperties.isEmpty()) {
            getLog().warn("No expected properties were loaded from " + this.expectedPropertyNames);
        }

        return expectedProperties;
    }

    public MavenProject getProject() {
        return project;
    }

    public void setProject(MavenProject project) {
        this.project = project;
    }

    public File getExpectedPropertyNames() {
        return expectedPropertyNames;
    }

    public void setExpectedPropertyNames(File expectedPropertyNames) {
        this.expectedPropertyNames = expectedPropertyNames;
    }

    public File getConfiguredPropertyNames() {
        return configuredPropertyNames;
    }

    public void setConfiguredPropertyNames(File configuredPropertyNames) {
        this.configuredPropertyNames = configuredPropertyNames;
    }
}

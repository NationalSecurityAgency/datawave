package datawave.maven;

import org.apache.commons.lang.StringUtils;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
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

/**
 * @goal assert-properties
 * @phase validate
 * @threadSafe true
 */
@SuppressWarnings("unused")
public class AssertProperties extends AbstractMojo {
    private static final Character COMMENT = '#', COMMA = ',';

    /**
     * @parameter default-value="${project}"
     * @required
     * @readonly
     */
    private MavenProject project;

    /**
     * @parameter
     * @required
     */
    private File expectedPropertyNames;

    /**
     * @parameter
     */
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

        if (expectedProperties.size() > 0) {
            StringBuilder errorMessage = new StringBuilder();
            errorMessage.append(expectedProperties.size() + " properties were not provided:\n");
            for (Entry<String,String> entry : getExpectedPropertyMap.entrySet()) {
                errorMessage.append("Missing property: " + entry.getKey() + ", Description: " + entry.getValue()).append("\n");
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
            FileReader propReader = null;
            envProps = new Properties();
            try {
                propReader = new FileReader(configuredPropertyNames);
                envProps.load(propReader);
            } catch (IOException e) {
                throw new MojoExecutionException("Could not load configuredPropertyNames", e);
            } finally {
                // Make sure we don't leave any open file handles laying around
                if (null != propReader) {
                    try {
                        propReader.close();
                    } catch (IOException e) {
                        throw new MojoExecutionException("Could not load configuredPropertyNames", e);
                    }
                }
            }
        }

        for (Entry<Object,Object> entry : buildProps.entrySet()) {
            envProps.put(entry.getKey(), entry.getValue());
        }

        return envProps;
    }

    /**
     * Fetch the set of strings from the configured filename
     * @return
     * @throws MojoExecutionException
     * @throws IOException
     */
    protected Map<String,String> getExpectedPropertyMap() throws MojoExecutionException {
        BufferedReader reader ;
        try {
            reader = new BufferedReader(new FileReader(this.expectedPropertyNames));
        } catch (FileNotFoundException e) {
            getLog().warn("Could not read exepcted properties files");

            throw new MojoExecutionException("Could not read expected properties file", e);
        }

        HashMap<String, String> expectedProperties = new HashMap<>();

        String line = null;
        try {
            while ((line = reader.readLine()) != null) {
                // Remove leading/trailing whitespace
                line = line.trim();

                // Ignore empty lines or those starting with a '#'
                if (StringUtils.isBlank(line) || line.charAt(0) == COMMENT) {
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
                if (StringUtils.isNotBlank(candidateName)) {
                    expectedProperties.put(candidateName, candidateDescription);
                }
            }
        } catch (IOException e) {
            throw new MojoExecutionException("Could not read expected properties file", e);
        } finally {
            // Make sure we don't leave any open file handles laying around
            try {
                reader.close();
            } catch (IOException e) {
                throw new MojoExecutionException("Could not close reader to expected properties", e);
            }
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

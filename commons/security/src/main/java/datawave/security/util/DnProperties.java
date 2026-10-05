package datawave.security.util;

import java.io.InputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.StringJoiner;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class contains properties that are often required when parsing/manipulating DNs.
 */
public class DnProperties {

    private static final Logger log = LoggerFactory.getLogger(DnProperties.class);

    /** Config for specifying default subject DN patterns and NPE OUs */
    public static final String DEFAULT_PROPERTIES_FILE = "dnutils.properties";

    /** System property that contains a regex pattern that matches against subject DNs. */
    public static final String SUBJECT_DN_PATTERN_PROPERTY = "subject.dn.pattern";

    /** System property containing a comma-delimited list of NPE OUs. */
    public static final String NPE_OU_PROPERTY = "npe.ou.entries";

    /**
     * The default instance loaded from system properties and/or a properties file.
     */
    private static DnProperties defaultInstance;

    private final Pattern subjectDnPattern;
    private final Set<String> npeOUs;

    /**
     * Return the default instance of {@link DnProperties}. If it does not exist, it will be set to the result of {@link #createInstanceFromProperties(String)}
     * with the properties file {@value DEFAULT_PROPERTIES_FILE}.
     *
     * @return the default {@link DnProperties}
     * @see #createInstanceFromProperties(String)
     */
    public static DnProperties getDefaultInstance() {
        if (defaultInstance == null) {
            defaultInstance = createInstanceFromProperties(DEFAULT_PROPERTIES_FILE);
        }
        return defaultInstance;
    }

    /**
     * Create an instance of {@link DnProperties} where the subject DN pattern and NPE OU list are loaded (in priority order) from the system properties
     * {@value SUBJECT_DN_PATTERN_PROPERTY} and {@value NPE_OU_PROPERTY}, or those same properties in the given properties file from the classloader if they
     * were not specified in the system properties.
     *
     * @return a new {@link DnProperties}
     */
    public static DnProperties createInstanceFromProperties(String propertiesFile) {
        // Attempt to fetch a subject DN pattern and NPE OU list from system properties.
        String subjectDnPatternValue = System.getProperty(SUBJECT_DN_PATTERN_PROPERTY, "");
        String npeOUsValue = System.getProperty(NPE_OU_PROPERTY, "");

        // If either a subject DN pattern or NPE OU list were not specified, attempt to load them from the properties file.
        if ((subjectDnPatternValue.isBlank()) || (npeOUsValue.isBlank())) {
            // Attempt to load the default subject DN pattern and NPE OU list from the properties file available via the classloader.
            Properties props = new Properties();
            try (InputStream in = DnProperties.class.getClassLoader().getResourceAsStream(propertiesFile)) {
                props.load(in);

                // Update the subject DN if needed.
                if (subjectDnPatternValue.isBlank()) {
                    subjectDnPatternValue = props.getProperty(SUBJECT_DN_PATTERN_PROPERTY, "");
                    if (subjectDnPatternValue.isBlank()) {
                        log.warn("Subject DN pattern property {} not set in {}", SUBJECT_DN_PATTERN_PROPERTY, propertiesFile);
                    }
                }

                // Update the NPE OU list if needed.
                if (npeOUsValue.isBlank()) {
                    npeOUsValue = props.getProperty(NPE_OU_PROPERTY, "");
                    if (npeOUsValue.isBlank()) {
                        log.warn("NPE OUs list property {} not set in {}", NPE_OU_PROPERTY, propertiesFile);
                    }
                }

            } catch (Exception e) {
                // If we failed to load the properties file, throw an error.
                log.error("Failed to load properties file {}", propertiesFile, e);
                throw new RuntimeException("Failed to load properties file " + propertiesFile, e);
            }

            // Throw an error if the subject DN is still blank.
            if (subjectDnPatternValue.isBlank()) {
                throw new IllegalArgumentException("Failed to load valid subject DN pattern from property " + SUBJECT_DN_PATTERN_PROPERTY
                                + " from system or properties file " + propertiesFile);
            }

            // Throw an error if the NPE OU list is still blank.
            if (npeOUsValue.isBlank()) {
                throw new IllegalArgumentException(
                                "Failed to load valid NPE OU list from property " + NPE_OU_PROPERTY + " from system or properties file " + propertiesFile);
            }
        }

        // Compile the subject DN pattern.
        Pattern subjectDnPattern;
        try {
            subjectDnPattern = Pattern.compile(subjectDnPatternValue, Pattern.CASE_INSENSITIVE);
        } catch (Throwable t) {
            log.error("{} = '{}' could not be compiled", SUBJECT_DN_PATTERN_PROPERTY, subjectDnPatternValue, t);
            throw new RuntimeException("Unable to compile subject DN pattern '" + subjectDnPatternValue + "'", t);
        }

        // Parse the NPE OU list.
        // @formatter:off
        List<String> npeOUs = Arrays.stream(npeOUsValue.split(","))
                        .map(String::trim)
                        .map(String::toUpperCase)
                        .collect(Collectors.toList());
        // @formatter:on

        return new DnProperties(subjectDnPattern, npeOUs);
    }

    /**
     * Create a new {@link DnProperties} from the given subject DN pattern and NPE OUs.
     *
     * @param subjectDnPattern
     *            the subject DN patterns
     * @param npeOUs
     *            the NPE OUs
     */
    public DnProperties(Pattern subjectDnPattern, Collection<String> npeOUs) {
        this.subjectDnPattern = subjectDnPattern;
        this.npeOUs = npeOUs.stream().map(String::trim).map(String::toUpperCase).collect(Collectors.toUnmodifiableSet());
    }

    /**
     * Return the subject DN pattern
     *
     * @return the subject DN pattern
     */
    public Pattern getSubjectDnPattern() {
        return subjectDnPattern;
    }

    /**
     * Return the set of NPE OUs, in all uppercase.
     *
     * @return the NPE OUs
     */
    public Set<String> getNpeOUs() {
        return npeOUs;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", DnProperties.class.getSimpleName() + "[", "]").add("subjectDnPattern=" + subjectDnPattern).add("npeOUs=" + npeOUs)
                        .toString();
    }
}

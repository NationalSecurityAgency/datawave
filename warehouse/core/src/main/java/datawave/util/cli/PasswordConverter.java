package datawave.util.cli;

import com.beust.jcommander.IStringConverter;

/**
 * Custom converter for parsing passwords from the command line. Inspired from Accumulo's ShellOptionsJC.PasswordConverter Allows password to be specified as an
 * environment variable using the form: env:PASSWORD_ENV_VAR, where PASSWORD_ENV_VAR is an environment variable that is accessible to the JVM
 *
 * <pre>
 * To us as a jcommander parameter:
 * </pre>
 *
 * <pre>
 * &#064;Parameter(names = {&quot;-p&quot;, &quot;--password&quot;}, description = &quot;password (if prefixed with env:NAME, NAME will be used to get from the environment)&quot;,
 *                 converter = PasswordConverter.class)
 * private String password;
 * </pre>
 *
 * Otherwise call {@code PasswordConverter.parseArg(passwordArgString)} to get same functionality
 */
public class PasswordConverter implements IStringConverter<String> {

    private static final String ENV_PREFIX = "env:";

    @Override
    public String convert(String value) {
        return parseArg(value);
    };

    /**
     * Static method to parse args that may contain env: prefix indicating that an environment variable should be looked up to obtain the value
     *
     * @param arg
     *            to parse
     * @return the parsed password or null if env var not defined
     */
    public static String parseArg(String arg) {
        return parseArg(arg, null);
    }

    /**
     * Static method to parse args that may contain env: prefix indicating that an environment variable should be looked up to obtain the value
     *
     * @param arg
     *            to parse
     * @param defaultValue
     *            if env var not present
     * @return the parsed password or default if env var not defined
     */
    public static String parseArg(String arg, String defaultValue) {
        if (arg.startsWith(ENV_PREFIX)) {
            String value = System.getenv(arg.substring(ENV_PREFIX.length()));
            if (null == value) {
                return defaultValue;
            } else {
                return value;
            }
        } else {
            return arg;
        }
    }
}

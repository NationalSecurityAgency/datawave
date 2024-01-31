package datawave.ingest.data.config;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;

import com.google.common.collect.Sets;

/**
 * Helper class that performs variable interpolation on keys and values in the Hadoop Configuration object
 *
 *
 *
 */
public class ConfigurationHelper extends Configuration {

    /**
     * Replaces all occurrences of match in the original configuration's set of key's and values to replacement and returns a new configuration object.
     *
     * @param orig
     *            the original configuration
     * @param regex
     *            the keys anhd values to try and match
     * @param replacement
     *            the string to replace the matches on the regex
     * @return new Configuration object with variables interpolated.
     */
    public static Configuration interpolate(Configuration orig, String regex, String replacement) {
        Configuration conf = new Configuration();
        for (Entry<String,String> e : orig) {
            String key = e.getKey().replaceAll(regex, replacement);
            String value = e.getValue().replaceAll(regex, replacement);
            conf.set(key, value);
        }
        return conf;
    }

    /**
     * Helper method to get properties from Hadoop configuration
     *
     * @param <T>
     *            Type of the value of property
     * @param conf
     *            the configuration to pull from
     * @param propertyName
     *            the name of the property
     * @param resultClass
     *            the class of the property
     * @throws IllegalArgumentException
     *             if property is not defined, null, or empty. Or if resultClass is not handled.
     * @return value of property
     */
    @SuppressWarnings("unchecked")
    public static <T> T isNull(Configuration conf, String propertyName, Class<T> resultClass) {
        String p = conf.get(propertyName);
        if (StringUtils.isEmpty(p))
            throw new IllegalArgumentException(propertyName + " must be specified");

        if (resultClass.equals(String.class))
            return (T) p;
        else if (resultClass.equals(String[].class))
            return (T) conf.getStrings(propertyName);
        else if (resultClass.equals(Boolean.class))
            return (T) Boolean.valueOf(p);
        else if (resultClass.equals(Long.class))
            return (T) Long.valueOf(p);
        else if (resultClass.equals(Integer.class))
            return (T) Integer.valueOf(p);
        else if (resultClass.equals(Float.class))
            return (T) Float.valueOf(p);
        else if (resultClass.equals(Double.class))
            return (T) Double.valueOf(p);
        else
            throw new IllegalArgumentException(resultClass.getSimpleName() + " is unhandled.");

    }

    public static void set(final Configuration conf, final String name, final Object value) {
        if (null == value) {
            return;
        }

        if (value instanceof String) {
            conf.set(name, (String) value);
        } else if (value instanceof Integer) {
            conf.setInt(name, ((Integer) value).intValue());
        } else if (value instanceof Long) {
            conf.setLong(name, ((Long) value).longValue());
        } else if (value instanceof Float) {
            conf.setFloat(name, ((Float) value).floatValue());
        } else if (value instanceof Double) {
            conf.setDouble(name, ((Double) value).doubleValue());
        } else if (value instanceof Boolean) {
            conf.setBoolean(name, ((Boolean) value).booleanValue());
        } else {
            throw (new IllegalArgumentException(value.getClass().getSimpleName() + " is unhandled."));
        }
    }

    /**
     * Adds the parameter to the configuration object, but only if the value is non-null.
     *
     * @param conf
     *            configuration object to update
     * @param propertyName
     *            the parameter name
     * @param value
     *            the parameter value
     * @param delimiter
     *            object upon which to separate the strings
     */
    public static void setSetOfStrings(final Configuration conf, final String propertyName, final Set<String> value, final String delimiter) {
        if (value != null) {
            conf.set(propertyName, StringUtils.join(value, delimiter));
        }
    }

    /**
     * Returns the parameter as a set, assuming the value is a comma-separated string. An exception is thrown if the value is null.
     *
     * @param conf
     *            configuration object
     * @param propertyName
     *            parameter name
     * @param delimiter
     *            delimiter
     * @return parameter value
     */
    public static Set<String> getRequiredSetOfStrings(final Configuration conf, final String propertyName, final String delimiter) {
        final String p = conf.get(propertyName);
        if (StringUtils.isEmpty(p))
            throw new IllegalArgumentException(propertyName + " must be specified");

        final String compositeValue = p.trim();
        if (StringUtils.isEmpty(compositeValue)) {
            return (new HashSet<>());
        }

        return (Sets.newHashSet(compositeValue.split(delimiter)));
    }

    /**
     * Get the value of the <code>name</code> property as a <code>List</code> of objects implementing the interface specified by <code>xface</code>.
     *
     * An exception is thrown if any of the classes does not exist, or if it does not implement the named interface.
     *
     * @param <U>
     *            Type of list
     * @param name
     *            name of property
     * @param conf
     *            the configuration name.
     * @param xface
     *            the interface implemented by the classes named by <code>name</code>.
     * @return a <code>List</code> of objects implementing <code>xface</code>.
     */
    @SuppressWarnings("unchecked")
    public static <U> List<U> getInstances(Configuration conf, String name, Class<U> xface) {
        List<U> ret = new ArrayList<>();
        Class<?>[] classes = conf.getClasses(name);
        for (Class<?> cl : classes) {
            if (!xface.isAssignableFrom(cl)) {
                throw new RuntimeException(cl + " does not implement " + xface);
            }
            ret.add((U) ReflectionUtils.newInstance(cl, conf));
        }
        return ret;
    }

}

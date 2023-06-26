package datawave.ingest.metadata.id;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import datawave.ingest.data.RawRecordContainer;

import org.apache.hadoop.util.StringUtils;

import com.google.common.collect.Multimap;

/**
 * A metadata key parser implementation will parse metadata out of a key.
 *
 */
public abstract class MetadataIdParser {

    public MetadataIdParser() {

    }

    private Pattern pattern = null;

    public MetadataIdParser(String pattern) {
        this.pattern = Pattern.compile(pattern);
    }

    public Matcher getMatcher(String key) {
        return pattern.matcher(key);
    }

    /**
     * This is the method that is called to parse metadata from an event
     *
     * @param event
     *            the event
     * @param key
     *            the key
     * @param metadata
     *            a map of metadata
     * @throws Exception
     *             if there is an issue
     */
    public abstract void addMetadata(RawRecordContainer event, Multimap<String,String> metadata, String key) throws Exception;

    /**
     * Create a metadata parser using all string arguments
     *
     * @param description
     *            This is a description of the form &lt;classname&gt;(args) which will be used to construct the parser. args should be of the form "..." {,
     *            "..."}.
     * @return A metadata parser.
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    public static MetadataIdParser createParser(String description) throws IllegalArgumentException {
        int beginArgs = description.indexOf('(');
        if (beginArgs <= 0) {
            throw new IllegalArgumentException("Expected classname(args): " + description);
        }
        if (!description.endsWith(")")) {
            throw new IllegalArgumentException("Expected classname(args): " + description);
        }
        String classname = description.substring(0, beginArgs);
        String argStr = description.substring(beginArgs + 1, description.length() - 1);
        try {
            Class parserClass = Class.forName(classname);
            if (!MetadataIdParser.class.isAssignableFrom(parserClass)) {
                throw new IllegalArgumentException("Expected extension of MetadataKeyParser class but got " + classname);
            }
            Object[] args = parseArgs(argStr);
            Class[] argClasses = new Class[args.length];
            for (int i = 0; i < args.length; i++)
                argClasses[i] = String.class;
            Constructor constructor = parserClass.getConstructor(argClasses);
            return (MetadataIdParser) (constructor.newInstance(args));
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException("Could not find class for " + classname, e);
        } catch (SecurityException | IllegalAccessException e) {
            throw new IllegalArgumentException("Could not access class for " + classname, e);
        } catch (NoSuchMethodException e) {
            throw new IllegalArgumentException("Could not find specified constructor for " + description, e);
        } catch (InstantiationException | InvocationTargetException e) {
            throw new IllegalArgumentException("Error constructiong " + description, e);
        }
    }

    /**
     * Parse "..." {, "..."} into an array of string arguments (as Object[])
     *
     * @param args
     *            the arguments
     * @return Object[] An array of string objects
     * @throws IllegalArgumentException
     *             if there is an issue with the arguments
     */
    public static Object[] parseArgs(String args) throws IllegalArgumentException {
        List<String> argList = new ArrayList<>();
        String[] parts = StringUtils.split(args, '\\', ',');
        for (String part : parts) {
            part = part.trim();
            if (part.charAt(0) == '"' && part.charAt(part.length() - 1) == '"') {
                part = StringUtils.unEscapeString(part.substring(1, part.length() - 1));
                argList.add(part);
            } else {
                throw new IllegalArgumentException("Expected a list of strings separated by commas.  Commas within the strings must be escaped. " + part);
            }
        }
        return argList.toArray();
    }
}

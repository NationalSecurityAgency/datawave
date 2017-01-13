package nsa.datawave.poller.util;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.TypeHandler;

public class PollerUtils {
    private PollerUtils() {
        // prevent construction
    }
    
    /**
     * A convenience method to get the last specified value on the command line for an option instead of the first which CommandLine.getOptionValue returns.
     *
     * @param cl
     *            the command-line object containing option values
     * @param opt
     *            name of the option
     * @return The last option value for opt
     */
    public static String getLastOptionValue(CommandLine cl, String opt) {
        String[] values = cl.getOptionValues(opt);
        
        return (values == null) ? null : values[values.length - 1];
    }
    
    /**
     * A convenience method to get the last specified value on the command line for an option instead of the first which CommandLine.getOptionValue returns.
     *
     * @param cl
     *            the command-line object containing option values
     * @param opt
     *            name of the option
     * @param defaultValue
     *            is the default value to be returned if the option is not specified
     * @return The last option value for opt, or defaultValue if missing
     */
    public static String getLastOptionValue(CommandLine cl, String opt, String defaultValue) {
        String answer = getLastOptionValue(cl, opt);
        
        return (answer == null) ? defaultValue : answer;
    }
    
    /**
     * A convenience method to get the last specified value on the command line for an option instead of the first which CommandLine.getLastParsedOptionValue
     * returns. Return the <code>Object</code> type of this <code>Option</code>.
     *
     * @param cl
     *            the command-line object containing option values
     * @param opt
     *            the name of the option
     * @param optionType
     *            the type of the option value
     * @param defaultValue
     *            the default value to return if the option is not specified
     * @return the type of this <code>Option</code>
     */
    @SuppressWarnings("unchecked")
    public static <V> V getLastParsedOptionValue(CommandLine cl, String opt, Object optionType, V defaultValue) throws ParseException {
        String res = getLastOptionValue(cl, opt);
        
        if (res == null)
            return defaultValue;
        
        // Check a couple types that the default TypeHandler doesn't
        if (optionType == Integer.class)
            return (V) Integer.valueOf(res);
        else if (optionType == Long.class)
            return (V) Long.valueOf(res);
        else
            return (V) TypeHandler.createValue(res, optionType);
    }
    
    /**
     * A convenience method to get the last specified value on the command line for an option instead of the first which CommandLine.getLastParsedOptionValue
     * returns. Return the <code>Object</code> type of this <code>Option</code>.
     *
     * @param cl
     *            the command-line object containing option values
     * @param opt
     *            the name of the option
     * @param optionType
     *            the type of the option value
     * @param defaultValue
     *            the default value to return if the option is not specified
     * @return the type of this <code>Option</code>
     */
    public static <V> V getLastParsedOptionValue(CommandLine cl, String opt, Class<V> optionType, V defaultValue) throws ParseException {
        return getLastParsedOptionValue(cl, opt, (Object) optionType, defaultValue);
    }
    
}

package datawave.common.cl;

import org.apache.commons.cli.Option;

/**
 * A helper for generating Command-Line Options. Unlike Apache's OptionBuilder, this one doesn't use a singleton and it maintains state between Option creation.
 * This allows developers to quickly and easily a bunch of Options with similar properties more easily.
 */
public class OptionBuilder {
    
    /** Number of arguments the next Option expects; defaults to 0. */
    private int args;
    
    /** The character separator for the next Option's aguments; defaults to none. */
    private char valSeparator;
    
    /** Whether or not the next option is required; defaults to false. */
    private boolean required;
    
    /** Whether or not the next Option's argument is optional; defaults to false. */
    private boolean optionalArg;
    
    /** The next Option's type; defaults to {@link String#getClass()}. */
    public Class<?> type = String.class;
    
    /**
     * Creates an Option using OptionBuilder's State and the given parameters.
     *
     * @param opt
     *            short representation of the option
     * @param desc
     *            descibes the function of the option
     * @return the new Option
     */
    public Option create(final String opt, final String desc) {
        return create(opt, null, desc);
    }
    
    /**
     * Creates an Option using OptionBuilder's State and the given parameters.
     *
     * @param opt
     *            short representation of the option
     * @param longOpt
     *            long representation of the option
     * @param desc
     *            descibes the function of the option
     * @return the new Option
     */
    public Option create(final String opt, final String longOpt, final String desc) {
        final Option option = new Option(opt, desc);
        option.setLongOpt(longOpt);
        option.setArgs(args);
        option.setRequired(required);
        option.setOptionalArg(optionalArg);
        option.setType(type);
        option.setValueSeparator(valSeparator);
        
        return option;
    }
    
    /** Resets the OptionBuilder. */
    public void reset() {
        args = 0;
        required = false;
        optionalArg = false;
        valSeparator = 0;
        type = String.class;
    }
    
    public int getArgs() {
        return args;
    }
    
    public char getValSeparator() {
        return valSeparator;
    }
    
    public boolean isRequired() {
        return required;
    }
    
    public boolean isOptionalArg() {
        return optionalArg;
    }
    
    public void setArgs(int args) {
        this.args = args;
    }
    
    public void setValSeparator(char valSeparator) {
        this.valSeparator = valSeparator;
    }
    
    public void setRequired(boolean required) {
        this.required = required;
    }
    
    public void setOptionalArg(boolean optionalArg) {
        this.optionalArg = optionalArg;
    }
    
}

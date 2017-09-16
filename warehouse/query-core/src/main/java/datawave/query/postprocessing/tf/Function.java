package datawave.query.postprocessing.tf;

import java.util.Collections;
import java.util.List;

import org.apache.commons.lang.builder.HashCodeBuilder;

import com.google.common.collect.Lists;

/**
 * Represents a function in a JEXL expression. Functions have a name a list of arguments associated with them. Functions are agnostic with regards to the
 * namespace in which they exist.
 */
public class Function {
    private String name;
    private List<String> args;
    
    public Function(String name, Iterable<String> args) {
        this.name = name;
        this.args = Lists.newArrayList(args);
        this.args = Collections.unmodifiableList(this.args);
    }
    
    public String name() {
        return name;
    }
    
    public List<String> args() {
        return args;
    }
    
    @Override
    public int hashCode() {
        HashCodeBuilder builder = new HashCodeBuilder();
        builder.append(name);
        for (String arg : args) {
            builder.append(arg);
        }
        return builder.toHashCode();
    }
    
    public String toString() {
        return name + args;
    }
}

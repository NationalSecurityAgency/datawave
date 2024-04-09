package datawave.core.query.postprocessing.tf;

import java.util.Collections;
import java.util.List;

import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import com.google.common.collect.Lists;

import datawave.core.query.jexl.visitors.PrintingVisitor;

/**
 * Represents a function in a JEXL expression. Functions have a name a list of arguments associated with them. Functions are agnostic with regards to the
 * namespace in which they exist.
 */
public class Function {
    private String name;
    private List<JexlNode> args;

    public Function(String name, Iterable<JexlNode> args) {
        this.name = name;
        this.args = Lists.newArrayList(args);
        this.args = Collections.unmodifiableList(this.args);
    }

    public String name() {
        return name;
    }

    public List<JexlNode> args() {
        return args;
    }

    @Override
    public int hashCode() {
        HashCodeBuilder builder = new HashCodeBuilder();
        builder.append(name);
        for (JexlNode arg : args) {
            builder.append(PrintingVisitor.formattedQueryString(arg));
        }
        return builder.toHashCode();
    }

    public String toString() {
        return name + args;
    }
}

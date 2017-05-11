package datawave.query.rewrite.jexl;

import org.apache.commons.jexl2.JexlArithmetic;

/**
 * a marker interface for a JexlArithmetic that has state. Used to prevent caching and sharing of the same instance. Stateful arithmetics needs to be cloneable
 * to create separate instances to be used concurrently.
 */
public interface StatefulArithmetic extends Cloneable {
    public JexlArithmetic clone();
}

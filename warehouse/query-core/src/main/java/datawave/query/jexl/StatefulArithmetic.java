package datawave.query.jexl;

import org.apache.commons.jexl3.JexlArithmetic;

/**
 * a marker interface for a JexlArithmetic that has state. Used to prevent caching and sharing of the same instance. Stateful arithmetics needs to be cloneable
 * to create separate instances to be used concurrently.
 */
public interface StatefulArithmetic extends Cloneable {
    JexlArithmetic clone();
}

package datawave.query.config;

import java.util.function.Function;

/**
 * When a ScanHintRule.apply() returns true the rule is applicable, false the rule hint can be skipped. If isChainable() is true rules can continue to be
 * evaluated after a successful apply(). If false evaluation should be halted.
 *
 * @param <T>
 */
public interface ScanHintRule<T> extends Function<T,Boolean> {
    /**
     * A ScanHintRule is chainable if when apply() returns true any remaining rules should be evaluated
     *
     * @return true if other rules should be evaluated if this rule apply()=true, false if no other ScanHintRules should be evaluated beyond this rule
     */
    boolean isChainable();

    /**
     * Get the table the hint is applied to when apply()=true
     *
     * @return the table the hint
     */
    String getTable();

    /**
     * The hint name to use when apply()=true
     *
     * @return
     */
    String getHintName();

    /**
     * The hint value to use when apply()=true
     *
     * @return
     */
    String getHintValue();
}

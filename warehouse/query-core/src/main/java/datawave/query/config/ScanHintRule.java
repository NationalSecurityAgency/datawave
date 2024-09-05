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
     * @return true if other rules should be evaluated if this rule is applicable {@link #apply(Object)} false if no other ScanHintRules should be evaluated if
     *         this rule applies
     */
    boolean isChainable();

    /**
     * Get the table this hint should be applied to
     *
     * @return the table name for the hint
     */
    String getTable();

    /**
     * Set the table name this hint should be applied to
     *
     * @param table
     *            the table name
     */
    void setTable(String table);

    /**
     * Get hint name for this rule
     *
     * @return the hint name
     */
    String getHintName();

    /**
     * Set the hint name for this rule
     *
     * @param hintName
     *            the hint name
     */
    void setHintName(String hintName);

    /**
     * The hint value
     *
     * @return the value of the hint
     */
    String getHintValue();

    /**
     * Set the hint value
     *
     * @param hintValue
     *            the hint value
     */
    void setHintValue(String hintValue);

    /**
     * Determine if this scan hint rule is applicable for o
     *
     * @param o
     *            the function argument
     * @return true if this scan hint rule is applicable, false otherwise
     */
    Boolean apply(T o);
}

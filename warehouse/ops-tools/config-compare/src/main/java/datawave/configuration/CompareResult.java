package datawave.configuration;

import java.util.Collections;
import java.util.Collection;

/**
 * Houses the results from running a comparison on two different type configurations.
 * <p>
 * Prefixes will be removed on prefixed fields. Example: "dataflow1.helper.classes" will be reported as "helper.classes".
 */
public class CompareResult {

    private final Collection<String> same;
    private final Collection<String> diff;
    private final Collection<String> leftOnly;
    private final Collection<String> rightOnly;

    public CompareResult(Collection<String> same, Collection<String> diff, Collection<String> leftOnly, Collection<String> rightOnly) {
        this.same = Collections.unmodifiableCollection(same);
        this.diff = Collections.unmodifiableCollection(diff);
        this.leftOnly = Collections.unmodifiableCollection(leftOnly);
        this.rightOnly = Collections.unmodifiableCollection(rightOnly);
    }

    /**
     * @return Fields that had the same value in both configurations.
     */
    public Collection<String> getSame() {
        return same;
    }

    /**
     * @return Fields that had different values (but existed) in both configurations.
     */
    public Collection<String> getDiff() {
        return diff;
    }

    /**
     * @return Fields that were only in the 'left' configuration.
     */
    public Collection<String> getLeftOnly() {
        return leftOnly;
    }

    /**
     * @return Fields that were only in the 'right' configuration.
     */
    public Collection<String> getRightOnly() {
        return rightOnly;
    }
}

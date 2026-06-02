package datawave.query.tables.ssdeep;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

import datawave.util.ssdeep.NGramTuple;
import datawave.util.ssdeep.SSDeepHash;

public class SSDeepSeenFunction implements Predicate<Map.Entry<NGramTuple,SSDeepHash>> {
    private final Set<Integer> seen;

    public SSDeepSeenFunction() {
        this(new HashSet<>());
    }

    public SSDeepSeenFunction(Set<Integer> seen) {
        this.seen = seen;
    }

    @Override
    public boolean test(Map.Entry<NGramTuple,SSDeepHash> entry) {
        int hashCode = entry.getValue().hashCode();
        if (seen.contains(hashCode)) {
            return false;
        }
        seen.add(hashCode);

        return true;
    }
}

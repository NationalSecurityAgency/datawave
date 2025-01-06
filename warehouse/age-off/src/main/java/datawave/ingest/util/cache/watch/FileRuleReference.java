package datawave.ingest.util.cache.watch;

import java.util.Collection;
import java.util.stream.Collectors;

import org.apache.accumulo.core.iterators.IteratorEnvironment;

import datawave.iterators.filter.ageoff.AppliedRule;
import datawave.iterators.filter.ageoff.FilterRule;

class FileRuleReference {
    private final long ts;
    private final Collection<FilterRule> rulesBase;

    FileRuleReference(long ts, Collection<FilterRule> rulesBase) {
        this.ts = ts;
        this.rulesBase = rulesBase;
    }

    public long getTimestamp() {
        return ts;
    }

    public Collection<AppliedRule> deepCopy(long scanStart, IteratorEnvironment iterEnv) {
        return rulesBase.stream().map(rule -> (AppliedRule) rule.deepCopy(scanStart, iterEnv)).collect(Collectors.toList());
    }
}

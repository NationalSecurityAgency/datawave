package datawave.mr.bulk.split;

import java.util.List;

import org.apache.accumulo.core.data.Range;

import com.google.common.collect.Iterables;

public class DefaultSplitStrategy extends SplitStrategy {

    private int splitCount = 0;

    public DefaultSplitStrategy(final int splitCount) {
        this.splitCount = splitCount;
    }

    public DefaultSplitStrategy() {
        this(Integer.MAX_VALUE);
    }

    @Override
    public Iterable<List<Range>> partition(Iterable<Range> iterable) {
        return Iterables.partition(iterable, splitCount);
    }

}

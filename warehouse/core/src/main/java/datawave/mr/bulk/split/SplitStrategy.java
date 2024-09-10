package datawave.mr.bulk.split;

import java.util.List;

import org.apache.accumulo.core.data.Range;

public abstract class SplitStrategy {

    public abstract Iterable<List<Range>> partition(Iterable<Range> iterable);
}

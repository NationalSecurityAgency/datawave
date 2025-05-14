package datawave.query.tables.keyword;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.accumulo.core.data.Range;

public class KeywordQueryState {
    final Set<String> viewNames = new HashSet<>();
    final Map<String,String> documentLanguageMap = new HashMap<>();
    final Collection<Range> ranges = new TreeSet<>();

    public Map<String,String> getDocumentLanguageMap() {
        return documentLanguageMap;
    }

    public Set<String> getViewNames() {
        return viewNames;
    }

    public void addRange(final Range range) {
        if (null != range) {
            this.ranges.add(range);
        }
    }

    public Collection<Range> getRanges() {
        return new ArrayList<>(this.ranges);
    }

    public void setRanges(final Collection<Range> ranges) {
        // As a single atomic operation, clear the range and add all of the
        // specified ranges
        this.ranges.clear();
        if (null != ranges) {
            this.ranges.addAll(ranges);
        }
    }
}

package datawave.query.util;

import static datawave.query.util.DateIndexHelper.DateTypeDescription;

import java.io.IOException;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

import com.google.common.base.Splitter;

import datawave.util.StringUtils;

/**
 * An iterator used by {@link DateIndexHelper#getTypeDescription(String, Date, Date, Set)}.
 * <p>
 * Supports filtering by datatype
 * <p>
 * Finds all matching fields and the min and max date
 */
public class DateTypeDescriptionIterator implements SortedKeyValueIterator<Key,Value> {

    public static final String DATATYPE_FILTER = "datatypes";

    private static final Splitter splitter = Splitter.on(',');

    private Set<String> datatypes;

    private SortedKeyValueIterator<Key,Value> source;

    private Key tk;
    private Value tv;

    private DateTypeDescription description = new DateTypeDescription();

    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        this.source = source;

        if (options.containsKey(DATATYPE_FILTER)) {
            String option = options.get(DATATYPE_FILTER);
            datatypes = new HashSet<>(splitter.splitToList(option));
        }
    }

    @Override
    public boolean hasTop() {
        return tk != null;
    }

    @Override
    public void next() throws IOException {
        tk = null;
        tv = null;
        // run the full scan
        while (source.hasTop()) {
            tk = source.getTopKey();
            String[] parts = StringUtils.split(tk.getColumnQualifier().toString(), '\0');

            if (datatypes == null || datatypes.isEmpty() || datatypes.contains(parts[1])) {
                description.ensureStartAndEndDateIsSet(parts[0]);
                description.addField(parts[2]);
            }

            source.next();
        }

        if (tk != null) {
            tv = new Value(description.serializeToString());
        }
    }

    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        source.seek(range, columnFamilies, inclusive);
        next();
    }

    @Override
    public Key getTopKey() {
        return tk;
    }

    @Override
    public Value getTopValue() {
        return tv;
    }

    @Override
    public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
        DateTypeDescriptionIterator iter = new DateTypeDescriptionIterator();
        iter.source = this.source.deepCopy(env);
        iter.description = this.description;
        return iter;
    }
}

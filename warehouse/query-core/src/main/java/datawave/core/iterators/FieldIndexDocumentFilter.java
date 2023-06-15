package datawave.core.iterators;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Map;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.commons.lang.StringUtils;

/**
 * This is a filter for field index keys for a specified datatype and uid.
 */
public class FieldIndexDocumentFilter extends Filter {
    protected static final String NULL_BYTE = "\0";
    public static final String DATA_TYPE_OPT = "dataType";
    public static final String EVENT_UID_OPT = "eventUid";
    private byte[] cqSuffix = null;

    @Override
    public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
        FieldIndexDocumentFilter newFilter = (FieldIndexDocumentFilter) (super.deepCopy(env));
        newFilter.cqSuffix = this.cqSuffix;
        return newFilter;
    }

    @Override
    public boolean accept(Key key, Value value) {
        ByteSequence cq = key.getColumnQualifierData();
        if (cq.length() < cqSuffix.length) {
            return false;
        }
        int pos = cq.length() - 1;
        for (int i = cqSuffix.length - 1; i >= 0; i--) {
            if (cq.byteAt(pos--) != cqSuffix[i]) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        super.init(source, options, env);
        String suffix = NULL_BYTE + options.get(DATA_TYPE_OPT) + NULL_BYTE + options.get(EVENT_UID_OPT);
        try {
            cqSuffix = suffix.getBytes("UTF8");
        } catch (UnsupportedEncodingException uee) {
            throw new RuntimeException("Unable to encode using UTF8?", uee);
        }
    }

    @Override
    public IteratorOptions describeOptions() {
        IteratorOptions options = super.describeOptions();
        options.addNamedOption(DATA_TYPE_OPT, "the data type");
        options.addNamedOption(EVENT_UID_OPT, "the event uid");
        return options;
    }

    @Override
    public boolean validateOptions(Map<String,String> options) {
        return (super.validateOptions(options) && options.containsKey(DATA_TYPE_OPT) && !StringUtils.isEmpty(options.get(DATA_TYPE_OPT))
                        && options.containsKey(EVENT_UID_OPT) && !StringUtils.isEmpty(options.get(EVENT_UID_OPT)));
    }
}

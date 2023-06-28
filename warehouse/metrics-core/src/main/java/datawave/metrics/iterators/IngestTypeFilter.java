package datawave.metrics.iterators;

import java.io.IOException;
import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

import datawave.metrics.keys.IngestEntryKey;
import datawave.metrics.util.WritableUtil;

public class IngestTypeFilter extends Filter {

    protected enum IngestType {
        LIVE, BULK, BOTH
    }

    protected IngestType type;

    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        super.init(source, options, env);

        String opt = options.get("type");
        if (opt == null) {
            type = IngestType.BOTH;
        } else {
            if ("live".equalsIgnoreCase(opt)) {
                type = IngestType.LIVE;
            } else if ("bulk".equalsIgnoreCase(opt)) {
                type = IngestType.BULK;
            } else {
                type = IngestType.BOTH;
            }
        }
    }

    @Override
    public boolean accept(Key k, Value v) {
        try {
            IngestTypeXKey itk = new IngestTypeXKey(k);
            switch (type) {
                case LIVE:
                    return itk.isLive();
                case BULK:
                    return itk.isBulk();
                default:
                    return true;
            }
        } catch (NullPointerException npe) {
            return false;
        }
    }

    /*
     * Optimization to avoid parsing the entire key each time
     */
    private static class IngestTypeXKey extends IngestEntryKey {

        public IngestTypeXKey(Key key) {
            super();
            parse(key);
        }

        @Override
        public void parse(Key k) {
            int pos = WritableUtil.findNth(k.getColumnQualifier(), 1, (byte) 0x0);
            this.live = (pos == -1);

            this.meAsAKey = k;
        }
    }

}

package datawave.metrics.iterators;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

import datawave.metrics.keys.AnalyticEntryKey;
import datawave.metrics.keys.InvalidKeyException;

public class AnalyticIngestTypeFilter extends IngestTypeFilter {
    private AnalyticEntryKey aek = new AnalyticEntryKey();

    @Override
    public boolean accept(Key k, Value v) {
        try {
            aek.parse(k);
            switch (type) {
                case LIVE:
                    return aek.isLive();
                case BULK:
                    return aek.isBulk();
                default:
                    return true;
            }

        } catch (InvalidKeyException e) {
            return false;
        }
    }
}

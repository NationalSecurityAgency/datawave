package datawave.ingest.table.aggregator;

import java.util.TreeSet;

import org.apache.accumulo.core.data.Value;
import org.apache.log4j.Logger;

import com.google.protobuf.InvalidProtocolBufferException;

import datawave.ingest.protobuf.TermWeight;
import datawave.ingest.protobuf.TermWeightPosition;

/**
 * An Aggregator to merge together a list of term offsets and one normalized term frequency This aggregator does <b>not</b> allow duplicate term offsets and
 * will do its best to remove any duplicate offsets and correct the normalized term frequency.
 *
 *
 *
 */
public class TextIndexAggregator extends PropogatingCombiner {
    private static final Logger log = Logger.getLogger(TextIndexAggregator.class);

    private TreeSet<TermWeightPosition> offsets = new TreeSet<>();
    private TermWeight.Info.Builder builder = TermWeight.Info.newBuilder();

    @Override
    public Value aggregate() {
        for (TermWeightPosition offset : offsets) {
            builder.addTermOffset(offset.getOffset());
            if (0 <= offset.getPrevSkips()) {
                builder.addPrevSkips(offset.getPrevSkips());
            }

            if (0 <= offset.getScore()) {
                builder.addScore(offset.getScore());
            }

            // If the zeroOffset has been set and the termweight is still default(true)
            if (builder.getZeroOffsetMatch()) {
                builder.setZeroOffsetMatch(offset.getZeroOffsetMatch());
            }
        }

        return new Value(builder.build().toByteArray());
    }

    /**
     * Determines whether or not to propagate the key depending on the result of the value
     *
     * @return a boolean on whether to propogate or not
     */
    public boolean propogateKey() {
        return true;
    }

    @Override
    public void collect(Value value) {
        // Make sure we don't aggregate something else
        if (value == null || value.get().length == 0) {
            return;
        }

        TermWeight.Info info;

        try {
            info = TermWeight.Info.parseFrom(value.get());
        } catch (InvalidProtocolBufferException e) {
            log.error("Value passed to aggregator was not of type TermWeight.Info", e);
            return;
        }

        // Add each offset into the list maintaining sorted order
        TermWeightPosition.Builder builder = new TermWeightPosition.Builder();
        for (int i = 0; i < info.getTermOffsetCount(); i++) {
            builder.setTermWeightOffsetInfo(info, i);
            offsets.add(builder.build());
            builder.reset();
        }
    }

    @Override
    public void reset() {
        this.offsets.clear();
        this.builder = TermWeight.Info.newBuilder();
    }

}

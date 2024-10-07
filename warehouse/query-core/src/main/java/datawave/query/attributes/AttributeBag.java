package datawave.query.attributes;

import java.io.Serializable;
import java.util.Collection;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.lang.mutable.MutableLong;
import org.apache.log4j.Logger;

import com.google.common.collect.Sets;

import datawave.marking.MarkingFunctions;
import datawave.marking.MarkingFunctions.Exception;

public abstract class AttributeBag<T extends Comparable<T>> extends Attribute<T> implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final Logger log = Logger.getLogger(AttributeBag.class);
    protected long shardTimestamp = Long.MAX_VALUE;
    protected boolean validMetadata = false;

    private static final long ONE_DAY_MS = 1000l * 60 * 60 * 24;

    public MarkingFunctions getMarkingFunctions() {
        return MarkingFunctions.Factory.createMarkingFunctions();
    }

    protected AttributeBag() {
        this(true);
    }

    public AttributeBag(boolean toKeep) {
        super(null, toKeep);
    }

    public AttributeBag(Key metadata, boolean toKeep) {
        super(metadata, toKeep);
    }

    public void invalidateMetadata() {
        this.validMetadata = false;
    }

    public boolean isValidMetadata() {
        return (this.validMetadata && isMetadataSet());
    }

    public abstract Collection<Attribute<? extends Comparable<?>>> getAttributes();

    @Override
    public long getTimestamp() {
        // calling isMetadataSet first to update the metadata as needed
        if (isValidMetadata() == false)
            this.updateMetadata();
        return super.getTimestamp();
    }

    @Override
    public ColumnVisibility getColumnVisibility() {
        if (isValidMetadata() == false)
            this.updateMetadata();
        return super.getColumnVisibility();
    }

    private void updateMetadata() {
        long ts = updateTimestamps();
        ColumnVisibility vis = super.getColumnVisibility();
        try {
            vis = this.combineAndSetColumnVisibilities(getAttributes());
        } catch (Exception e) {
            log.error("got error combining visibilities", e);
        }
        setMetadata(vis, ts);
        validMetadata = true;
    }

    protected ColumnVisibility combineAndSetColumnVisibilities(Collection<Attribute<? extends Comparable<?>>> attributes) throws Exception {
        Collection<ColumnVisibility> columnVisibilities = Sets.newHashSet();
        for (Attribute<?> attr : attributes) {
            columnVisibilities.add(attr.getColumnVisibility());
        }
        return MarkingFunctions.Factory.createMarkingFunctions().combine(columnVisibilities);
    }

    private long updateTimestamps() {
        MutableLong ts = new MutableLong(Long.MAX_VALUE);
        for (Attribute<?> attribute : getAttributes()) {
            mergeTimestamps(attribute, ts);
        }
        return ts.longValue();
    }

    private void mergeTimestamps(Attribute<?> other, MutableLong ts) {
        // if this is a set of attributes, then examine each one. Note not recursing on a Document as it should have already applied the shard time.
        if (other instanceof AttributeBag) {
            // recurse on the sub attributes
            for (Attribute<?> attribute : ((AttributeBag<?>) other).getAttributes()) {
                mergeTimestamps(attribute, ts);
            }
        } else if (other.isMetadataSet()) {
            // if this is the first attribute being merged
            if (ts.longValue() == Long.MAX_VALUE) {
                // if we know the shard time
                if (shardTimestamp != Long.MAX_VALUE) {
                    // if the timestamp is outside of the shard's date, then set to the end of the day
                    if (other.getTimestamp() < shardTimestamp) {
                        log.error("Found an attribute of a document with a timestamp prior to the shardId date! " + shardTimestamp + " vs " + other);
                        ts.setValue(shardTimestamp + ONE_DAY_MS - 1);
                    } else if (other.getTimestamp() >= (shardTimestamp + ONE_DAY_MS)) {
                        log.debug("Found an attribute of a document with a timestamp ofter the shardId date, ignoring: " + shardTimestamp + " vs " + other);
                        ts.setValue(shardTimestamp + ONE_DAY_MS - 1);
                    }
                    // else simply use the new timestamp
                    else {
                        ts.setValue(other.getTimestamp());
                    }
                } else {
                    ts.setValue(other.getTimestamp());
                }
            }
            // else this is not the first attribute being merged
            else {
                // if we know the shard time
                if (shardTimestamp != Long.MAX_VALUE) {
                    // if the new timestamp is before the shard's date, then ignore it
                    if (other.getTimestamp() < shardTimestamp) {
                        log.error("Found an attribute of a document with a timestamp prior to the shardId date! " + other);
                    }
                    // else update the timestamp with the min value
                    else {
                        ts.setValue(Math.min(ts.longValue(), other.getTimestamp()));
                    }
                }
                // else update the timestamp with the min value
                else {
                    ts.setValue(Math.min(ts.longValue(), other.getTimestamp()));
                }
            }
        }
    }

    @Override
    public void setToKeep(boolean toKeep) {
        super.setToKeep(toKeep);
        // do not change values of child attributes to avoid overriding earlier decisions
    }
}

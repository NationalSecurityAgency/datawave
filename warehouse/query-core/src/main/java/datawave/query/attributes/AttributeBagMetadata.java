package datawave.query.attributes;

import java.io.Serializable;
import java.util.Collection;
import java.util.stream.Collectors;

import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.lang.mutable.MutableLong;
import org.apache.log4j.Logger;

import datawave.marking.MarkingFunctions;

public class AttributeBagMetadata extends AttributeMetadata implements Serializable {
    private static final long serialVersionUID = -5961455715747661898L;
    private static final Logger log = Logger.getLogger(AttributeBagMetadata.class);
    protected long shardTimestamp = Long.MAX_VALUE;
    protected boolean validMetadata = false;
    protected MarkingFunctions<?> markingFunctions;

    private static final long ONE_DAY_MS = 1000l * 60 * 60 * 24;

    public interface AttributesGetter {
        Collection<Attribute<? extends Comparable<?>>> getAttributes();

        Collection<Attribute<? extends Comparable<?>>> getRawAttributes();
    }

    public MarkingFunctions<?> getMarkingFunctions() {
        if (null == markingFunctions) {
            markingFunctions = MarkingFunctions.Factory.createMarkingFunctions();
        }
        return markingFunctions;
    }

    private final AttributesGetter attributes;

    public AttributeBagMetadata(AttributesGetter attributes) {
        this.attributes = attributes;
    }

    public boolean isNotValidMetadata() {
        return (!this.validMetadata || !isMetadataSet());
    }

    public boolean isValidMetadata() {
        return !isNotValidMetadata();
    }

    public void invalidateMetadata() {
        setValidMetadata(false);
    }

    public void setValidMetadata(boolean valid) {
        this.validMetadata = valid;
    }

    public long getShardTimestamp() {
        return shardTimestamp;
    }

    public void setShardTimestamp(long shardTimestamp) {
        this.shardTimestamp = shardTimestamp;
    }

    @Override
    public long getTimestamp() {
        // calling isMetadataSet first to update the metadata as needed
        if (isNotValidMetadata())
            this.updateMetadata();
        return super.getTimestamp();
    }

    public ColumnVisibility getColumnVisibility() {
        if (isNotValidMetadata())
            this.updateMetadata();
        return super.getColumnVisibility();
    }

    private void updateMetadata() {
        long ts = updateTimestamps();
        ColumnVisibility vis = super.getColumnVisibility();
        try {
            vis = this.combineAndSetColumnVisibilities(attributes.getRawAttributes());
        } catch (MarkingFunctions.Exception e) {
            log.error("got error combining visibilities", e);
        }
        setMetadata(vis, ts);
        setValidMetadata(true);
    }

    protected ColumnVisibility combineAndSetColumnVisibilities(Collection<Attribute<? extends Comparable<?>>> attributes) throws MarkingFunctions.Exception {
        Collection<ColumnVisibility> visibilities = attributes.stream().map(Attribute::getColumnVisibility).collect(Collectors.toList());
        return getMarkingFunctions().combineVisibilities(visibilities);
    }

    private long updateTimestamps() {
        MutableLong ts = new MutableLong(Long.MAX_VALUE);
        for (Attribute<?> attribute : attributes.getRawAttributes()) {
            mergeTimestamps(attribute, ts);
        }
        return ts.longValue();
    }

    private void mergeTimestamps(Attribute<?> other, MutableLong ts) {
        // if this is a set of attributes, then examine each one. Note not recursing on a Document as it should have already applied the shard time.
        if (other instanceof Attributes) {
            // recurse on the sub attributes
            for (Attribute<?> attribute : ((Attributes) other).getRawAttributes()) {
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

}

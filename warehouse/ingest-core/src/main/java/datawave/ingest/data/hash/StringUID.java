package datawave.ingest.data.hash;

import com.google.common.base.Objects;

import datawave.data.hash.HashUID;
import datawave.data.hash.SnowflakeUID;
import datawave.data.hash.UID;
import datawave.data.hash.UIDConstants;
import datawave.util.StringUtils;

/**
 * Extension of UID to accept only the shard ID from a previous event
 */
public class StringUID extends HashUID {

    protected String innerUid = null;
    protected String shardedPortion = null;

    public StringUID(String uid) {
        innerUid = uid;
        final UID parsed = (null != innerUid) ? UID.parse(innerUid) : null;
        if (parsed instanceof SnowflakeUID) {
            shardedPortion = ((SnowflakeUID) parsed).getShardedPortion();
        } else {
            // Legacy code
            String[] split = StringUtils.split(innerUid, UIDConstants.DEFAULT_SEPARATOR);
            if (split.length == 4) {
                shardedPortion = split[0] + UIDConstants.DEFAULT_SEPARATOR + split[1] + UIDConstants.DEFAULT_SEPARATOR + split[2];
            } else {
                shardedPortion = innerUid;
            }
        }
    }

    /**
     * Get the portion of the UID to be used for sharding (@see datawave.ingest.mapreduce.handler.shard.ShardIdFactory)
     *
     * @return a portion of the UID for sharding
     */
    @Override
    public String getShardedPortion() {
        return shardedPortion;
    }

    @Override
    public boolean equals(final Object o) {
        boolean equals;
        if (!(o instanceof StringUID)) {
            equals = false;
        } else {
            final StringUID other = (StringUID) o;
            equals = super.equals(other) && innerUid.equals(other.innerUid) && shardedPortion.equals(other.shardedPortion);
        }

        return equals;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(super.hashCode(), innerUid, shardedPortion);
    }

    @Override
    public String toString() {
        return innerUid;
    }
}

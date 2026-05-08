package datawave.query.discovery;

import java.util.Optional;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import com.google.protobuf.InvalidProtocolBufferException;

import datawave.ingest.protobuf.Uid;
import datawave.query.Constants;

public class TermOnlyEntry implements TermInterface {

    private final String term;
    private final String field;
    private String date;
    private final ColumnVisibility visibility;
    private long uidCount;
    private long uidListSize;
    private boolean valid;

    public TermOnlyEntry(Key key, Value value) {
        term = key.getRow().toString();
        field = key.getColumnFamily().toString();
        visibility = new ColumnVisibility(key.getColumnVisibility());

        String colq = key.getColumnQualifier().toString();
        int firstSeparatorPos = colq.indexOf(Constants.NULL_BYTE_STRING);
        if (firstSeparatorPos != -1) {
            int lastSeparatorPos = colq.lastIndexOf(Constants.NULL_BYTE_STRING);
            // If multiple separators are present, this is a task datatype entry.
            if (firstSeparatorPos != lastSeparatorPos) {
                // Ensure that we at least have yyyyMMdd.
                if ((lastSeparatorPos - firstSeparatorPos) < 9) {
                    return;
                }
                // The form is datatype\0date\0task status (old knowledge entry).
                date = colq.substring(firstSeparatorPos + 1, firstSeparatorPos + 9);
                // datatype = colq.substring(0, firstSeparatorPos);
            } else {
                // Ensure that we at least have yyyyMMdd.
                if (firstSeparatorPos < 8) {
                    return;
                }
                // The form is shardId\0datatype.
                date = colq.substring(0, 8);
            }

            // Parse the UID.List object from the value.
            try {
                Uid.List uidList = Uid.List.parseFrom(value.get());
                if (uidList != null) {
                    uidCount = uidList.getCOUNT();
                    uidListSize = uidList.getUIDList().size();
                }
            } catch (InvalidProtocolBufferException e) {
                // Don't add UID information. At least we know what shard it's located in.
            }

            // Parse the UID.List object from the value.
            try {
                Uid.List uidList = Uid.List.parseFrom(value.get());
                if (uidList != null) {
                    uidCount = uidList.getCOUNT();
                    uidListSize = uidList.getUIDList().size();
                }
            } catch (InvalidProtocolBufferException e) {
                // Don't add UID information. At least we know what shard it's located in.
            }
        }

        // This is now considered a valid term entry for aggregation.
        valid = Optional.ofNullable(term).isPresent();
    }

    public String getTerm() {
        return this.term;
    }

    public String getField() {
        return field;
    }

    public String getDate() {
        return date;
    }

    public ColumnVisibility getVisibility() {
        return visibility;
    }

    public long getUidCount() {
        return uidCount;
    }

    public long getUidListSize() {
        return uidListSize;
    }

    public boolean isValid() {
        return this.valid;
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof TermOnlyEntry) {
            TermOnlyEntry other = (TermOnlyEntry) o;
            // @formatter:off
            return new EqualsBuilder().append(getTerm(), other.getTerm())
                    .append(getVisibility(), other.getVisibility()).isEquals();
            // @formatter:on
        }
        return false;
    }

    @Override
    public int hashCode() {
        // @formatter:off
        return new HashCodeBuilder().append(getTerm())
                .append(getVisibility()).toHashCode();
        // @formatter:on
    }
}

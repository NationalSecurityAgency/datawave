package datawave.query.discovery;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;

import com.google.protobuf.InvalidProtocolBufferException;

import datawave.ingest.protobuf.Uid;
import datawave.query.Constants;

public class TermInfo {
    protected long count = 0;
    protected String fieldValue = null;
    protected String fieldName = null;
    protected String date = null;
    protected String datatype = null;
    protected ColumnVisibility vis = null;
    protected boolean valid = false;
    private long listSize = 0;

    public TermInfo(Key key, Value value) {
        // Get the shard id and datatype from the colq
        fieldValue = key.getRow().toString();
        fieldName = key.getColumnFamily().toString();
        String colq = key.getColumnQualifier().toString();

        int separator = colq.indexOf(Constants.NULL_BYTE_STRING);
        if (separator != -1) {
            int end_separator = colq.lastIndexOf(Constants.NULL_BYTE_STRING);
            // if we have multiple separators, then we must have a tasking data type entry.
            if (separator != end_separator) {
                // ensure we at least have yyyyMMdd
                if ((end_separator - separator) < 9) {
                    return;
                }
                // in this case the form is datatype\0date\0task status (old knowledge entry)
                date = colq.substring(separator + 1, separator + 9);
                datatype = colq.substring(0, separator);
            } else {
                // ensure we at least have yyyyMMdd
                if (separator < 8) {
                    return;
                }
                // in this case the form is shardid\0datatype
                date = colq.substring(0, 8);
                datatype = colq.substring(separator + 1);
            }

            // Parse the UID.List object from the value
            Uid.List uidList = null;
            try {
                uidList = Uid.List.parseFrom(value.get());
                if (null != uidList) {
                    count = uidList.getCOUNT();
                    setListSize(uidList.getUIDList().size());
                }
            } catch (InvalidProtocolBufferException e) {
                // Don't add UID information, at least we know what shard
                // it is located in.
            }

            Text tvis = key.getColumnVisibility();
            vis = new ColumnVisibility(tvis);

            // we now have a valid info
            valid = true;
        }
    }

    private void setListSize(int size) {
        listSize = size;

    }

    public long getListSize() {
        return listSize;
    }
}

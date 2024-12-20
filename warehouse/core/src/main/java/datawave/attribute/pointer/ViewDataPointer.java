package datawave.attribute.pointer;

import org.apache.accumulo.core.data.Key;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ViewDataPointer implements DataPointer {
    private static final String D_COLUMN = "d";

    // this is for deserialization see DataPointer
    private final String type = "dView";

    @JsonProperty
    private String shard;

    @JsonProperty
    private String dataType;

    @JsonProperty
    private String uid;

    @JsonProperty
    private String view;

    public ViewDataPointer() {
        // no-op
    }

    public ViewDataPointer(String shard, String dataType, String uid, String view) {
        this.shard = shard;
        this.dataType = dataType;
        this.uid = uid;
        this.view = view;
    }

    @Override
    public Key get() {
        return new Key(shard, D_COLUMN, dataType + '\u0000' + uid + '\u0000' + view);
    }

    public String getShard() {
        return this.shard;
    }

    public String getDataType() {
        return this.dataType;
    }

    public String getUid() {
        return this.uid;
    }

    public String getView() {
        return this.view;
    }
}

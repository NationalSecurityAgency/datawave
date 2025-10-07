package datawave.ingest.wikipedia;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

/**
 *
 */
public class WikipediaPage implements Writable {
    protected int id;
    protected String title;
    protected long timestamp;
    protected String comments;
    protected String text;

    public WikipediaPage(int id, String title, long timestamp, String comments, String text) {
        checkNotNull(id);
        checkNotNull(title);
        checkNotNull(timestamp);
        checkNotNull(comments);
        checkNotNull(text);

        this.id = id;
        this.title = title;
        this.timestamp = timestamp;
        this.comments = comments;
        this.text = text;
    }

    public int getId() {
        return this.id;
    }

    public String getTitle() {
        return this.title;
    }

    public String getComments() {
        return this.comments;
    }

    public String getText() {
        return this.text;
    }

    public long getTimestamp() {
        return this.timestamp;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.id = in.readInt();
        this.title = WritableUtils.readString(in);
        this.timestamp = in.readLong();
        this.comments = WritableUtils.readString(in);
        this.text = WritableUtils.readString(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(this.id);
        WritableUtils.writeString(out, this.title);
        out.writeLong(this.timestamp);
        WritableUtils.writeString(out, this.comments);
        WritableUtils.writeString(out, this.text);
    }
}

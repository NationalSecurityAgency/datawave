package datawave.ingest.mapreduce.job;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.accumulo.core.data.Key;
import org.apache.hadoop.io.DataOutputOutputStream;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import com.esotericsoftware.kryo.io.Output;

public class KryoBulkIngestKey implements WritableComparable {
    private Key key;
    private Text tableName;
    private static ThreadLocal<Output> outputLocal = ThreadLocal.withInitial(() -> new Output(4096));
    private static ThreadLocal<Text> dataHolder = ThreadLocal.withInitial(() -> new Text());

    public KryoBulkIngestKey() {
        this.tableName = new Text();
        this.key = new Key();
    }

    public KryoBulkIngestKey(Text tableName, Key key) {
        super();
        this.tableName = tableName;
        if (null == this.tableName) {
            this.tableName = new Text();
        }
        this.key = key;
    }

    @Override
    public int compareTo(Object o) {
        return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        var kryoOut = outputLocal.get();
        // var kryoOut = new Output(4096);
        try (var dos = DataOutputOutputStream.constructOutputStream(dataOutput)) {
            kryoOut.setOutputStream(dos);
            var t = dataHolder.get();
            kryoOut.write(tableName.getBytes(), 0, tableName.getLength());
            // key.getRow(t);
            // kryoOut.writeInt(t.getLength());
            // kryoOut.write(t.getBytes(), 0, t.getLength());
            // key.getColumnFamily(t);
            // kryoOut.writeInt(t.getLength());
            // kryoOut.write(t.getBytes(), 0, t.getLength());
            // key.getColumnQualifier(t);
            // kryoOut.writeInt(t.getLength());
            // kryoOut.write(t.getBytes(), 0, t.getLength());
            // key.getColumnVisibility(t);
            // kryoOut.writeInt(t.getLength());
            // kryoOut.write(t.getBytes(), 0, t.getLength());
            var rowArray = key.getRowData();
            var cfArray = key.getColumnFamilyData();
            var cqArray = key.getColumnQualifierData();
            var cvArray = key.getColumnVisibilityData();

            kryoOut.write(rowArray.getBackingArray(), rowArray.offset(), rowArray.length());
            kryoOut.write(cfArray.getBackingArray(), cfArray.offset(), cfArray.length());
            kryoOut.write(cqArray.getBackingArray(), cqArray.offset(), cqArray.length());
            kryoOut.write(cvArray.getBackingArray(), cvArray.offset(), cvArray.length());

            // kryoOut.write(tableName.getBytes(), 0, tableName.getLength());
            // Stream.of(rowArray, cfArray, cqArray, cvArray).forEach(array -> {
            // kryoOut.writeInt(array.length(), true);
            // kryoOut.writeBytes(array.getBackingArray(), array.offset(), array.length());
            // });
            kryoOut.writeLong(key.getTimestamp());
            kryoOut.flush();
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

    }
}

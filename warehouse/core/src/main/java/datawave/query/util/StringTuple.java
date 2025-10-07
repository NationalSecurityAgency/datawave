package datawave.query.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.WritableComparable;

public class StringTuple implements WritableComparable<StringTuple> {
    final List<String> values;

    public StringTuple(int size) {
        values = new ArrayList<>(size);
    }

    public StringTuple add(String s) {
        values.add(s);
        return this;
    }

    public String get(int i) {
        return values.get(i);
    }

    public int getSize() {
        return values.size();
    }

    public StringTuple clear() {
        values.clear();
        return this;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int num = in.readInt();

        if (num < 0) {
            return;
        }

        values.clear();
        for (int i = 0; i < num; ++i) {
            values.add(in.readUTF());
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(values.size());
        for (String s : values) {
            out.writeUTF(s);
        }
    }

    @Override
    public int compareTo(StringTuple tuple) {
        int min = Math.min(tuple.values.size(), values.size());

        for (int i = 0; i < min; ++i) {
            int cmp = values.get(i).compareTo(tuple.values.get(i));

            if (cmp > 0 || cmp < 0)
                return cmp;
        }

        if (values.size() < tuple.values.size()) {
            return -1;
        } else if (values.size() > tuple.values.size()) {
            return 1;
        } else {
            return 0;
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        for (String s : values) {
            sb.append(s).append("|");
        }
        sb.deleteCharAt(sb.length() - 1);

        return sb.toString();
    }

}

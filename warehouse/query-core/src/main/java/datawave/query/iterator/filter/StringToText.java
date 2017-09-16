package datawave.query.iterator.filter;

import org.apache.hadoop.io.Text;

import com.google.common.base.Function;

public class StringToText implements Function<String,Text> {
    @Override
    public Text apply(String from) {
        return new Text(from);
    }
}

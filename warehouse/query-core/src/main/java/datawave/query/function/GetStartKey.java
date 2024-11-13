package datawave.query.function;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;

import com.google.common.base.Function;

public class GetStartKey implements Function<Range,Key> {
    private static final GetStartKey inst = new GetStartKey();

    public static GetStartKey instance() {
        return inst;
    }

    @Override
    public Key apply(Range input) {
        return input.getStartKey();
    }

}

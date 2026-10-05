package datawave.query.tld;

import static datawave.table.util.TLD.buildParentKey;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;

import com.google.common.base.Function;

import datawave.table.util.TLD;

public class GetStartKeyForRoot implements Function<Range,Key> {
    private static final GetStartKeyForRoot inst = new GetStartKeyForRoot();

    public static GetStartKeyForRoot instance() {
        return inst;
    }

    @Override
    public Key apply(Range input) {
        Key k = input.getStartKey();
        return buildParentKey(k.getRow(), TLD.parseRootPointerFromId(k.getColumnFamilyData()), k.getColumnQualifierData(), k.getColumnVisibility(),
                        k.getTimestamp());
    }

}

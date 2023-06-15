package datawave.query.table.parser;

import java.util.SortedSet;
import java.util.TreeSet;

import com.google.protobuf.InvalidProtocolBufferException;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.lang3.StringUtils;

import datawave.ingest.protobuf.TermWeight;
import datawave.marking.MarkingFunctions;
import datawave.query.table.parser.EventKeyValueFactory.EventKeyValue;

public class TermFrequencyKeyValueFactory {

    public static TermFrequencyKeyValue parse(Key key, Value value, Authorizations auths, MarkingFunctions markingFunctions) throws MarkingFunctions.Exception {
        if (null == key) {
            throw new IllegalArgumentException("Cannot pass null key to TermFrequencyKeyValueFactory");
        }

        TermFrequencyKeyValue t = new TermFrequencyKeyValue();
        t.setShardId(key.getRow().toString());

        String[] field = StringUtils.split(key.getColumnQualifier().toString(), "\0");
        if (field.length > 0) {
            t.setDatatype(field[0]);
        }

        if (field.length > 1) {
            t.setUid(field[1]);
        }

        if (field.length > 2) {
            t.setFieldValue(field[2]);
        }

        if (field.length > 3) {
            t.setFieldName(field[3]);
        }

        parseColumnVisibility(t, key, auths, markingFunctions);

        if (null != value && value.get().length > 0) {
            try {
                TermWeight.Info termWeightInfo = TermWeight.Info.parseFrom(value.get());
                t.count = termWeightInfo.getTermOffsetCount();
                t.offsets = new TreeSet<>(termWeightInfo.getTermOffsetList());
            } catch (InvalidProtocolBufferException e) {
                throw new RuntimeException("Error deserializing Term Weight Information", e);
            }
        }
        return t;
    }

    protected static void parseColumnVisibility(TermFrequencyKeyValue tfkv, Key key, Authorizations auths, MarkingFunctions markingFunctions)
                    throws MarkingFunctions.Exception {
        tfkv.setMarkings(markingFunctions.translateFromColumnVisibilityForAuths(key.getColumnVisibilityParsed(), auths));
    }

    public static class TermFrequencyKeyValue extends EventKeyValue {
        protected int count = -1;
        protected SortedSet<Integer> offsets = null;

        public int getCount() {
            return count;
        }

        public SortedSet<Integer> getOffsets() {
            return offsets;
        }
    }
}

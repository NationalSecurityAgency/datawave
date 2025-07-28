package datawave.query.table.parser;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;

import datawave.marking.MarkingFunctions;
import datawave.query.Constants;
import datawave.query.table.parser.EventKeyValueFactory.EventKeyValue;
import datawave.util.StringUtils;

/** Parses results returned from the KeywordExtractingIterator. Expects 'd' column keys, and serialized bytes as the value */
public class KeywordKeyValueFactory {

    public static KeywordKeyValue parse(Key key, Value value, Authorizations auths, MarkingFunctions markingFunctions) throws MarkingFunctions.Exception {

        if (null == key)
            throw new IllegalArgumentException("Cannot pass null key to KeywordKeyValueFactory");
        if (null == value)
            throw new IllegalArgumentException("Cannot pass null value to KeywordKeyValueFactory");

        KeywordKeyValue k = new KeywordKeyValue();
        k.setShardId(key.getRow().toString());

        String[] cqParts = StringUtils.split(key.getColumnQualifier().toString(), Constants.NULL_BYTE_STRING);
        if (cqParts.length > 0) {
            k.setDatatype(cqParts[0]);
        }
        if (cqParts.length > 1) {
            k.setUid(cqParts[1]);
        }
        if (cqParts.length > 2) {
            k.setViewName(cqParts[2]);
        }
        if (value.get().length > 0) {
            k.setContents(value.get());
        }

        EventKeyValueFactory.parseColumnVisibility(k, key, auths, markingFunctions);

        return k;
    }

    public static class KeywordKeyValue extends EventKeyValue {

        protected String viewName = null;

        // todo: instead of relying on the transformer to interpret these bytes,
        // and turn them into KeywordResults, consider it here instead.
        protected byte[] contents = null;

        public String getViewName() {
            return viewName;
        }

        public byte[] getContents() {
            return contents;
        }

        protected void setViewName(String viewName) {
            this.viewName = viewName;
        }

        protected void setContents(byte[] contents) {
            this.contents = contents;
        }
    }
}

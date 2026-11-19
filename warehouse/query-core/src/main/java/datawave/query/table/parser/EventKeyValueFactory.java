package datawave.query.table.parser;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;

import datawave.query.Constants;
import datawave.visibility.AccessExpressionUtil;

public class EventKeyValueFactory {

    public static EventKeyValue parse(Key key, Value value) {

        if (null == key)
            throw new IllegalArgumentException("Cannot pass null key to EventKeyValueFactory");

        EventKeyValue e = new EventKeyValue();
        e.setShardId(key.getRow().toString());

        String[] parts = key.getColumnFamily().toString().split(Constants.NULL_BYTE_STRING);
        if (parts.length > 0)
            e.setDatatype(parts[0]);
        if (parts.length > 1)
            e.setUid(parts[1]);

        String[] field = key.getColumnQualifier().toString().split(Constants.NULL_BYTE_STRING);
        if (field.length > 0)
            e.setFieldName(field[0]);
        if (field.length > 1)
            e.setFieldValue(field[1]);

        e.setTimestamp(key.getTimestamp());

        parseColumnVisibility(e, key);

        return e;
    }

    protected static void parseColumnVisibility(EventKeyValue event, Key key) {
        event.setColumnVisibility(AccessExpressionUtil.normalize(new ColumnVisibility(key.getColumnVisibility())).getExpression());
    }

    public static class EventKeyValue {
        protected String shardId = null;
        protected String datatype = null;
        protected String uid = null;
        protected String fieldName = null;
        protected String fieldValue = null;
        protected String columnVisibility = null;
        protected long timestamp = 0L;

        public String getShardId() {
            return this.shardId;
        }

        public String getDatatype() {
            return this.datatype;
        }

        public String getUid() {
            return this.uid;
        }

        public String getFieldName() {
            return this.fieldName;
        }

        public String getFieldValue() {
            return this.fieldValue;
        }

        public long getTimestamp() {
            return this.timestamp;
        }

        protected void setShardId(String shardId) {
            this.shardId = shardId;
        }

        protected void setDatatype(String datatype) {
            this.datatype = datatype;
        }

        protected void setUid(String uid) {
            this.uid = uid;
        }

        protected void setFieldName(String fieldName) {
            this.fieldName = fieldName;
        }

        protected void setFieldValue(String fieldValue) {
            this.fieldValue = fieldValue;
        }

        protected void setTimestamp(long timestamp) {
            this.timestamp = timestamp;
        }

        /**
         * @return the normalized column visibility of the key
         */
        public String getColumnVisibility() {
            return this.columnVisibility;
        }

        public void setColumnVisibility(String columnVisibility) {
            this.columnVisibility = columnVisibility;
        }

    }

}

package datawave.core.query.configuration;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.hadoop.io.Text;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

/**
 * Class to encapsulate all required information to run a query.
 */
public class QueryData implements ResultContext, Externalizable {
    private String tableName;
    private String query;
    @JsonSerialize(using = RangeListSerializer.class)
    @JsonDeserialize(using = RangeListDeserializer.class)
    private Collection<Range> ranges = new HashSet<>();
    private Collection<String> columnFamilies = new HashSet<>();
    @JsonDeserialize(using = IteratorSettingListDeserializer.class)
    private List<IteratorSetting> settings = new ArrayList<>();
    @JsonSerialize(using = KeySerializer.class)
    @JsonDeserialize(using = KeyDeserializer.class)
    private Key lastResult;
    private boolean rebuildHashCode = true;
    private int hashCode = -1;
    boolean finished = false;

    public QueryData() {
        // empty constructor
    }

    /**
     * Full constructor
     *
     * @param tableName
     *            the table name
     * @param query
     *            the query string
     * @param ranges
     *            a collection of ranges
     * @param columnFamilies
     *            a collection of column families
     * @param settings
     *            a list of IteratorSetting
     */
    public QueryData(String tableName, String query, Collection<Range> ranges, Collection<String> columnFamilies, List<IteratorSetting> settings) {
        this.tableName = tableName;
        this.query = query;
        this.ranges = ranges;
        this.columnFamilies = columnFamilies;
        this.settings = settings;
    }

    /**
     * Copy constructor
     *
     * @param other
     *            another instance of QueryData
     */
    public QueryData(QueryData other) {
        this.tableName = other.tableName;
        this.query = other.query;
        this.ranges = new HashSet<>(other.ranges);
        this.columnFamilies = new HashSet<>(other.columnFamilies);
        this.settings = new ArrayList<>(other.settings);
        this.hashCode = other.hashCode;
        this.rebuildHashCode = other.rebuildHashCode;
        this.lastResult = other.lastResult;
        this.finished = finished;
    }

    @Deprecated(since = "6.5.0", forRemoval = true)
    public QueryData(String tableName, String query, Collection<Range> ranges, List<IteratorSetting> settings) {
        setTableName(tableName);
        setQuery(query);
        setRanges(ranges);
        setSettings(settings);
    }

    /**
     * Weak copy constructor that updates the ranges
     *
     * @param other
     *            another QueryData
     * @param ranges
     *            a collection of updated ranges
     * @deprecated
     */
    @Deprecated(since = "6.5.0", forRemoval = true)
    public QueryData(QueryData other, Collection<Range> ranges) {
        this(other);
        setRanges(ranges);
    }

    @Deprecated(since = "6.5.0", forRemoval = true)
    public QueryData(String tableName, String queryString, List<Range> ranges, List<IteratorSetting> settings, Collection<String> columnFamilies) {
        this(tableName, queryString, ranges, settings);
        this.columnFamilies.addAll(columnFamilies);
    }

    // builder style methods

    public QueryData withTableName(String tableName) {
        this.tableName = tableName;
        resetHashCode();
        return this;
    }

    public QueryData withQuery(String query) {
        this.query = query;
        resetHashCode();
        return this;
    }

    public QueryData withRanges(Collection<Range> ranges) {
        this.ranges = ranges;
        resetHashCode();
        return this;
    }

    public QueryData withColumnFamilies(Collection<String> columnFamilies) {
        this.columnFamilies = columnFamilies;
        resetHashCode();
        return this;
    }

    public QueryData withSettings(List<IteratorSetting> settings) {
        this.settings = settings;
        resetHashCode();
        return this;
    }

    public void setSettings(List<IteratorSetting> settings) {
        this.settings.clear();
        if (settings != null) {
            this.settings.addAll(settings);
        }
        resetHashCode();
    }

    public List<IteratorSetting> getSettings() {
        return settings;
    }

    public void setQuery(String query) {
        this.query = query;
        resetHashCode();
    }

    public String getQuery() {
        return query;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public Collection<Range> getRanges() {
        if (isFinished()) {
            return Collections.emptySet();
        } else if (lastResult != null) {
            List<Range> newRanges = new ArrayList<>();
            for (Range range : ranges) {
                if (range.contains(lastResult)) {
                    newRanges.add(new Range(lastResult, false, range.getEndKey(), range.isEndKeyInclusive()));
                } else {
                    newRanges.add(range);
                }
            }
            return newRanges;
        }
        return ranges;
    }

    public Collection<String> getColumnFamilies() {
        return columnFamilies;
    }

    public void setColumnFamilies(Collection<String> columnFamilies) {
        this.columnFamilies.clear();
        if (columnFamilies != null) {
            this.columnFamilies.addAll(columnFamilies);
        }
        resetHashCode();
    }

    public void addColumnFamily(String cf) {
        this.columnFamilies.add(cf);
        resetHashCode();
    }

    public void addColumnFamily(Text cf) {
        this.columnFamilies.add(cf.toString());
        resetHashCode();
    }

    public void setRanges(Collection<Range> ranges) {
        this.ranges.clear();
        if (null != ranges) {
            this.ranges.addAll(ranges);
        }
        resetHashCode();
    }

    public void addRange(Range range) {
        this.ranges.add(range);
        resetHashCode();
    }

    public void addIterator(IteratorSetting cfg) {
        this.settings.add(cfg);
        resetHashCode();
    }

    public void setLastResult(Key result) {
        this.lastResult = result;
        if (this.lastResult == null) {
            this.finished = true;
        }
        resetHashCode();
    }

    public boolean isFinished() {
        return this.finished;
    }

    public Key getLastResult() {
        return lastResult;
    }

    @Override
    public String toString() {
        //  @formatter:off
        return new StringBuilder()
                .append("Query: '").append(this.query)
                .append("', Ranges: ").append(this.ranges)
                .append(", lastResult: ").append(this.lastResult)
                .append(", Settings: ").append(this.settings)
                .toString();
        //  @formatter:on
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeUTF(tableName);
        out.writeInt(settings.size());
        for (IteratorSetting setting : settings) {
            setting.write(out);
        }
        if (query != null) {
            out.writeBoolean(true);
            out.writeUTF(query);
        } else {
            out.writeBoolean(false);
        }
        out.writeInt(ranges.size());
        for (Range range : ranges) {
            range.write(out);
        }
        out.writeInt(columnFamilies.size());
        for (String cf : columnFamilies) {
            out.writeUTF(cf);
        }
        if (lastResult != null) {
            out.writeBoolean(true);
            lastResult.write(out);
        } else {
            out.writeBoolean(false);
        }
        out.writeBoolean(finished);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException {
        tableName = in.readUTF();
        settings.clear();
        int count = in.readInt();
        for (int i = 0; i < count; i++) {
            settings.add(new IteratorSetting(in));
        }
        boolean exists = in.readBoolean();
        if (exists) {
            query = in.readUTF();
        }
        ranges.clear();
        count = in.readInt();
        for (int i = 0; i < count; i++) {
            Range range = new Range();
            range.readFields(in);
            ranges.add(range);
        }
        count = in.readInt();
        for (int i = 0; i < count; i++) {
            columnFamilies.add(in.readUTF());
        }
        exists = in.readBoolean();
        if (exists) {
            lastResult = new Key();
            lastResult.readFields(in);
        }
        finished = in.readBoolean();
    }

    public QueryData(ObjectInput in) throws IOException {
        readExternal(in);
    }

    @Override
    public int hashCode() {
        if (rebuildHashCode) {
            //  @formatter:off
            hashCode = new HashCodeBuilder()
                    .append(tableName)
                    .append(query)
                    .append(ranges)
                    .append(columnFamilies)
                    .append(settings)
                    .append(lastResult)
                    .append(finished)
                    .hashCode();
            rebuildHashCode = false;
            //  @formatter:on
        }
        return hashCode;
    }

    public boolean equals(Object o) {
        if (o instanceof QueryData) {
            QueryData other = (QueryData) o;
            //  @formatter:off
            return new EqualsBuilder()
                    .append(tableName, other.tableName)
                    .append(query, other.query)
                    .append(ranges, other.ranges)
                    .append(columnFamilies, other.columnFamilies)
                    .append(settings, other.settings)
                    .append(lastResult, other.lastResult)
                    .append(finished, other.finished)
                    .isEquals();
            //  @formatter:on
        }
        return false;
    }

    /**
     * Method to reset the hashcode when an internal variable is updated
     */
    private void resetHashCode() {
        rebuildHashCode = true;
    }

    /**
     * A json deserializer for a list of IteratorSetting which handles the json deserialization issues. The accumulo IteratorSetting does not have a default
     * constructor.
     */
    public static class IteratorSettingListDeserializer extends StdDeserializer<List<IteratorSetting>> {
        private ObjectMapper mapper = new ObjectMapper();

        public IteratorSettingListDeserializer() {
            this(null);
        }

        public IteratorSettingListDeserializer(Class<?> valueClass) {
            super(valueClass);
        }

        @Override
        public List<IteratorSetting> deserialize(JsonParser parser, DeserializationContext deserializer) throws IOException, JsonProcessingException {
            List<IteratorSetting> list = new ArrayList<>();
            ObjectCodec codec = parser.getCodec();
            JsonNode node = codec.readTree(parser);

            for (int i = 0; i < node.size(); i++) {
                list.add(getIteratorSetting(node.get(i)));
            }

            return list;
        }

        private IteratorSetting getIteratorSetting(JsonNode node) throws JsonProcessingException {
            IteratorSetting setting = new IteratorSetting(1, "a", "a");
            JsonNode child = node.get("priority");
            if (child != null) {
                setting.setPriority(child.asInt());
            }
            child = node.get("name");
            if (child != null) {
                setting.setName(child.asText());
            }
            child = node.get("iteratorClass");
            if (child != null) {
                setting.setIteratorClass(child.asText());
            }
            child = node.get("options");
            if (child == null) {
                child = node.get("properties");
            }
            if (child != null) {
                setting.addOptions(mapper.treeToValue(child, HashMap.class));
            }
            return setting;
        }
    }

    /**
     * A json deserializer for a list of Range which handles the json deserialization issues. The accumulo Range and Key classes do not have appropriate
     * setters.
     */
    public static class RangeListSerializer extends StdSerializer<Collection<Range>> {
        private ObjectMapper mapper = new ObjectMapper();

        public RangeListSerializer() {
            this(null);
        }

        public RangeListSerializer(Class<Collection<Range>> type) {
            super(type);
        }

        @Override
        public void serialize(Collection<Range> ranges, JsonGenerator jgen, SerializerProvider provider) throws IOException {
            jgen.writeStartArray(ranges == null ? 0 : ranges.size());
            if (ranges != null) {
                for (Range range : ranges) {
                    serialize(range, jgen, provider);
                }
            }
            jgen.writeEndArray();
        }

        public void serialize(Range range, JsonGenerator jgen, SerializerProvider provider) throws IOException {
            jgen.writeStartObject();
            if (range.getStartKey() != null) {
                jgen.writeFieldName("startKey");
                new KeySerializer().serialize(range.getStartKey(), jgen, provider);
            }
            jgen.writeBooleanField("startKeyInclusive", range.isStartKeyInclusive());
            if (range.getEndKey() != null) {
                jgen.writeFieldName("endKey");
                new KeySerializer().serialize(range.getEndKey(), jgen, provider);
            }
            jgen.writeBooleanField("endKeyInclusive", range.isEndKeyInclusive());
            jgen.writeEndObject();
        }
    }

    /**
     * A json deserializer for a list of Range which handles the json deserialization issues. The accumulo Range and Key classes do not have appropriate
     * setters.
     */
    public static class RangeListDeserializer extends StdDeserializer<Collection<Range>> {
        public RangeListDeserializer() {
            this(null);
        }

        public RangeListDeserializer(Class<?> valueClass) {
            super(valueClass);
        }

        @Override
        public Collection<Range> deserialize(JsonParser parser, DeserializationContext deserializer) throws IOException {
            ObjectCodec codec = parser.getCodec();
            JsonNode node = codec.readTree(parser);
            return deserialize(node);
        }

        public Collection<Range> deserialize(JsonNode node) throws IOException {
            Collection<Range> list = new ArrayList<>();
            for (int i = 0; i < node.size(); i++) {
                list.add(getRange(node.get(i)));
            }
            return list;
        }

        private Range getRange(JsonNode node) throws IOException {
            JsonNode start = node.get("startKey");
            JsonNode startInclusive = node.get("startKeyInclusive");
            JsonNode end = node.get("endKey");
            JsonNode endInclusive = node.get("endKeyInclusive");
            return new Range(getKey(start), startInclusive.asBoolean(), getKey(end), endInclusive.asBoolean());
        }

        private Key getKey(JsonNode node) throws IOException {
            return new KeyDeserializer().deserialize(node);
        }
    }

    /**
     * A json deserializer for a list of Range which handles the json deserialization issues. The accumulo Range and Key classes do not have appropriate
     * setters.
     */
    public static class KeySerializer extends StdSerializer<Key> {
        public KeySerializer() {
            this(null);
        }

        public KeySerializer(Class<Key> type) {
            super(type);
        }

        @Override
        public void serialize(Key key, JsonGenerator jgen, SerializerProvider provider) throws IOException {
            jgen.writeStartObject();
            jgen.writeBinaryField("row", key.getRowData().getBackingArray());
            jgen.writeBinaryField("cf", key.getColumnFamilyData().getBackingArray());
            jgen.writeBinaryField("cq", key.getColumnQualifierData().getBackingArray());
            jgen.writeBinaryField("cv", key.getColumnVisibility().getBytes());
            jgen.writeNumberField("ts", key.getTimestamp());
            jgen.writeBooleanField("d", key.isDeleted());
            jgen.writeEndObject();
        }
    }

    /**
     * A json deserializer for a list of Range which handles the json deserialization issues. The accumulo Range and Key classes do not have appropriate
     * setters.
     */
    public static class KeyDeserializer extends StdDeserializer<Key> {
        public KeyDeserializer() {
            this(null);
        }

        public KeyDeserializer(Class<?> type) {
            super(type);
        }

        @Override
        public Key deserialize(JsonParser parser, DeserializationContext deserializer) throws IOException, JsonProcessingException {
            ObjectCodec codec = parser.getCodec();
            JsonNode node = codec.readTree(parser);
            return deserialize(node);
        }

        public Key deserialize(JsonNode node) throws IOException {
            if (node == null) {
                return null;
            }
            JsonNode row = node.get("row");
            JsonNode cf = node.get("cf");
            JsonNode cq = node.get("cq");
            JsonNode cv = node.get("cv");
            JsonNode ts = node.get("ts");
            JsonNode d = node.get("d");
            return new Key(row.binaryValue(), cf.binaryValue(), cq.binaryValue(), cv.binaryValue(), ts.longValue(), d.booleanValue());
        }
    }
}

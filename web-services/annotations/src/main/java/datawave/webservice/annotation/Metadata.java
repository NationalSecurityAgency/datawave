package datawave.webservice.annotation;

import java.util.Objects;

/**
 * An immutable object that captures the internal Metadata associated with a record. It also provides a {@code equals} and {@code hashCode} implementation so
 * that it can be used as a {@code Map} key. Similar to the {@link datawave.webservice.query.result.event.Metadata} class, but adds features specific to working
 * with {@link datawave.annotation.protobuf.v1.Annotation} and {@link datawave.annotation.protobuf.v1.Segment}.
 */
public class Metadata {
    /** the table where the record is stored */
    private final String table;
    /** the accumulo row / shard associated with a record */
    private final String row;
    /** the datatype of a record */
    private final String dataType;
    /** the internal identifier for a record */
    private final String internalId;

    /**
     * Create a Metadata object using the field values from a {@link datawave.webservice.query.result.event.Metadata} object.
     *
     * @param eventMetadata
     *            the object whose fields to copy.
     */
    public Metadata(datawave.webservice.query.result.event.Metadata eventMetadata) {
        this(eventMetadata.getTable(), eventMetadata.getRow(), eventMetadata.getDataType(), eventMetadata.getInternalId());
    }

    /**
     * Create a Metatata object based on the specified field values
     *
     * @param table
     *            the table where the record is stored
     * @param row
     *            accumulo row / shard associated with a record
     * @param dataType
     *            the datatype of a record
     * @param internalId
     *            the internal identifier for a record
     */
    public Metadata(String table, String row, String dataType, String internalId) {
        if (table == null || row == null || dataType == null || internalId == null) {
            final String message = String.format("Null value provided when creating metadata: %s/%s/%s [%s]", row, dataType, internalId, table);
            throw new NullPointerException(message);
        }

        this.row = row;
        this.dataType = dataType;
        this.internalId = internalId;
        this.table = table;
    }

    public String getDataType() {
        return dataType;
    }

    public String getInternalId() {
        return internalId;
    }

    public String getRow() {
        return row;
    }

    public String getTable() {
        return table;
    }

    public String toString() {
        return String.format("%s/%s/%s [%s]", row, dataType, internalId, table);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass())
            return false;
        Metadata metadata = (Metadata) o;
        return Objects.equals(row, metadata.row) && Objects.equals(dataType, metadata.dataType) && Objects.equals(internalId, metadata.internalId)
                        && Objects.equals(table, metadata.table);
    }

    @Override
    public int hashCode() {
        return Objects.hash(row, dataType, internalId, table);
    }
}

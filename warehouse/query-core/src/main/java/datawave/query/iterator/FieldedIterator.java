package datawave.query.iterator;

/**
 * Interface to identify what field an iterator is related to. This will be used to identify IndexIterator's fields
 */
public interface FieldedIterator {
    /**
     *
     * @return the field related to this Iterator, or null if no specific field is associated with the iterator
     */
    String getField();
}

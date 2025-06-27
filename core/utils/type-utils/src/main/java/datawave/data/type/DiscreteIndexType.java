package datawave.data.type;

import java.util.List;

/**
 * Contains a collection of useful methods which can be used against index entries which are discrete and calculable.
 *
 * @param <T>
 */
public interface DiscreteIndexType<T extends Comparable<T>> extends Type<T> {

    /**
     * Increments the given index to the next logical value.
     * <p>
     * If producesFixedLengthRanges is true, and incrementIndex would cause the length of the index to change, the original index will be returned.
     *
     * @param index
     *            the index
     * @return an incremented index
     */
    String incrementIndex(String index);

    /**
     * Decrements the given index to the previous logical value.
     * <p>
     * If producesFixedLengthRanges is true, and decrementIndex would cause the length of the index to change, the original index will be returned.
     *
     * @param index
     *            the index
     * @return a decremented index
     */
    String decrementIndex(String index);

    /**
     * Returns a list of all discrete values between begin and end.
     * <p>
     * If producesFixedLengthRanges is true, the returned values will be of the same length as begin and end.
     * <p>
     * If producesFixedLengthRanges is true, and begin and end are of different lengths, the original range will be returned.
     * <p>
     * If begin does not come before end, an empty list will be returned.
     *
     * @param beginIndex
     *            the start index
     * @param endIndex
     *            the stop index
     * @return a list of the discrete index values between begin and end
     */
    List<String> discretizeRange(String beginIndex, String endIndex);

    /**
     * Indicates whether the ranges against the given indices will be of fixed length. That is to say, whether all index values within a given range will have
     * the same string length. This is an important characteristic which enables composite ranges to be created.
     *
     * @return whether query ranges against these values will be of fixed length
     */
    boolean producesFixedLengthRanges();
}

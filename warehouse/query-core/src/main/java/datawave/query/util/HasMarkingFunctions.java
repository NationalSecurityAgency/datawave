package datawave.query.util;

import datawave.marking.MarkingFunctions;

/**
 * @deprecated nothing uses {@link MarkingFunctions} any more; visibilities are combined with {@code datawave.visibility.VisibilityFunctions}
 */
// TODO remove-accumulo-utils
@Deprecated(forRemoval = true)
public interface HasMarkingFunctions {

    void setMarkingFunctions(MarkingFunctions<?> markingFunctions);

    MarkingFunctions<?> getMarkingFunctions();

}

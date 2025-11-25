package datawave.annotation.data;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;

public class AccumuloAnnotationUtil {
    /**
     * Utility method to transform an iterator of Accumulo Key, Value pairs to a single mutation. All keys must share the same rowId.
     *
     * @param it
     *            an iterator used to access key/value data.
     * @param delete
     *            if we should generate delete mutations instead of write mutations.
     * @return a Mutation containing the contents of the provided iterator or null of there was nothing in the iterator that could be used to generate a
     *         mutation.
     * @throws AnnotationSerializationException
     *             if the keys don't all share the same rowId.
     */
    public static Mutation mutationAdapter(Iterator<Map.Entry<Key,Value>> it, boolean delete) throws AnnotationSerializationException {
        if (it == null) {
            return null;
        }

        Map.Entry<byte[],Mutation> mutationState = null;

        while (it.hasNext()) {
            final Map.Entry<Key,Value> entry = it.next();
            final Key key = entry.getKey();

            byte[] currentRow = key.getRowData().toArray();

            if (mutationState == null) {
                mutationState = Map.entry(currentRow, new Mutation(currentRow));
            } else if (!Arrays.equals(mutationState.getKey(), currentRow)) {
                //@formatter:off
                throw new AnnotationSerializationException("The current row is not the same as the first row provided for the mutation: " +
                        "firstRow: [" + new String(mutationState.getKey()) + "] " +
                        "currentRow: [" + new String(currentRow) + "]");
                //@formatter:on
            }

            //@formatter:off
            final Mutation.MutationOptions mutationOptions = mutationState.getValue().at()
                    .family(key.getColumnFamilyData().toArray())
                    .qualifier(key.getColumnQualifierData().toArray())
                    .visibility(key.getColumnVisibilityParsed())
                    .timestamp(key.getTimestamp());
            //@formatter:on

            if (delete) {
                mutationOptions.delete();
            } else {
                mutationOptions.put(entry.getValue());
            }
        }
        return mutationState == null ? null : mutationState.getValue();
    }
}

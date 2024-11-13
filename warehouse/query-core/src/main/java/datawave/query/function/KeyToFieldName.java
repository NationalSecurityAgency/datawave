package datawave.query.function;

import java.nio.charset.CharacterCodingException;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;

import com.google.common.base.Function;
import com.google.common.collect.Maps;

/**
 * Function to transform the attributes for an event as {@code Entry<Key,Value>} to {@code Entry<Key,String>} where the Entry's value is the fieldName from the
 * Key <br>
 *
 *
 *
 */
public class KeyToFieldName implements Function<Entry<Key,Value>,Entry<Key,String>> {

    private boolean includeGroupingContext = false;

    public KeyToFieldName() {}

    /**
     * Constructor that sets the includeGroupingContext flag
     *
     * @param includeGroupingContext
     *            flag that determines if a field name should include the grouping context
     */
    public KeyToFieldName(boolean includeGroupingContext) {
        this.includeGroupingContext = includeGroupingContext;
    }

    /**
     * Transforms a key-value entry into a key-string entry
     *
     * @param from
     *            a key-value entry
     * @return a key-string entry
     */
    @Override
    public Entry<Key,String> apply(Entry<Key,Value> from) {
        String fieldName = getFieldName(from.getKey());
        return Maps.immutableEntry(from.getKey(), fieldName);
    }

    /**
     * Parse the field name from the key's column qualifier
     *
     * @param k
     *            the key
     * @return the field name
     */
    public String getFieldName(Key k) {

        int index = -1;

        ByteSequence bs = k.getColumnQualifierData();
        byte[] backing = bs.getBackingArray();

        for (int i = 0; i < bs.length(); i++) {
            if (backing[i] == 0x00 || (!includeGroupingContext && backing[i] == '.')) {
                index = i;
                break;
            }
        }

        if (0 > index) {
            throw new IllegalArgumentException("Could not find null-byte contained in ColumnQualifier for key: " + k);
        }

        try {
            return Text.decode(backing, 0, index);
        } catch (CharacterCodingException e) {
            throw new IllegalArgumentException(e);
        }
    }
}

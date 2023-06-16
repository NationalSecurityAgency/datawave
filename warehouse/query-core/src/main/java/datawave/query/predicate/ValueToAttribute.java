package datawave.query.predicate;

import java.nio.charset.CharacterCodingException;
import java.util.Map.Entry;

import datawave.query.attributes.Attribute;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.attributes.AttributeFactory;
import datawave.query.util.TypeMetadata;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import com.google.common.base.Function;
import com.google.common.collect.Maps;

/**
 * Transform the given fieldName/fieldValue (String/String) into String/Attribute
 *
 *
 *
 */
public class ValueToAttribute implements Function<Entry<Key,String>,Entry<Key,Entry<String,Attribute<? extends Comparable<?>>>>> {
    private static final Logger log = Logger.getLogger(ValueToAttribute.class);

    private AttributeFactory attrFactory;

    private Filter attrFilter;

    public ValueToAttribute(TypeMetadata typeMetadata, EventDataQueryFilter attrFilter) {
        this.attrFactory = new AttributeFactory(typeMetadata);
        this.attrFilter = attrFilter;
    }

    @Override
    public Entry<Key,Entry<String,Attribute<? extends Comparable<?>>>> apply(Entry<Key,String> from) {
        String origFieldName = from.getValue();
        String modifiedFieldName = JexlASTHelper.deconstructIdentifier(from.getValue(), false);
        return Maps.<Key,Entry<String,Attribute<? extends Comparable<?>>>> immutableEntry(from.getKey(),
                        Maps.<String,Attribute<? extends Comparable<?>>> immutableEntry(origFieldName, getFieldValue(modifiedFieldName, from.getKey())));
    }

    public Attribute<?> getFieldValue(String fieldName, Key k) {
        ByteSequence sequence = k.getColumnQualifierData();
        int index = -1;
        for (int i = 0; i < sequence.length(); i++) {
            if (sequence.byteAt(i) == 0x00) {
                index = i;
                break;
            }
        }

        if (0 > index) {
            throw new IllegalArgumentException("Could not find null-byte contained in columnqualifier for key: " + k);
        }

        try {
            String data = Text.decode(sequence.getBackingArray(), index + 1, (sequence.length() - (index + 1))).intern();

            Attribute<?> attr = this.attrFactory.create(fieldName, data, k, attrFilter == null || attrFilter.keep(k));
            if (attrFilter != null) {
                attr.setToKeep(attrFilter.keep(k));
            }

            return attr;
        } catch (CharacterCodingException e) {
            throw new IllegalArgumentException(e);
        }
    }

}

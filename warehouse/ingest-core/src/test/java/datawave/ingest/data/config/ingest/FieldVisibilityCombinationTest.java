package datawave.ingest.data.config.ingest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.Collection;

import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;

import datawave.ingest.data.Type;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.data.config.NormalizedFieldAndValue;

/**
 * Verifies how the per-field visibilities of member fields are combined into the visibility of composite and virtual fields. A null field visibility means "use
 * the event visibility" and must be preserved as null, never replaced by an empty (public) visibility.
 */
public class FieldVisibilityCombinationTest {

    private static final String TYPE_NAME = "test";
    private static final String FIELD_A = "FIELD_A";
    private static final String FIELD_B = "FIELD_B";
    private static final String COMPOSITE = "COMPOSITE_AB";
    private static final String VIRTUAL = "VIRTUAL_AB";

    private static Type type() {
        return new Type(TYPE_NAME, null, null, null, 1, null);
    }

    private static Multimap<String,NormalizedContentInterface> fields(ColumnVisibility visA, ColumnVisibility visB) {
        Multimap<String,NormalizedContentInterface> fields = HashMultimap.create();
        fields.put(FIELD_A, new NormalizedFieldAndValue(FIELD_A, "a", visA));
        fields.put(FIELD_B, new NormalizedFieldAndValue(FIELD_B, "b", visB));
        return fields;
    }

    private static ColumnVisibility compositeVisibility(ColumnVisibility visA, ColumnVisibility visB) {
        Configuration config = new Configuration();
        config.set(TYPE_NAME + "." + COMPOSITE + CompositeIngest.COMPOSITE_FIELD_MAP, FIELD_A + "," + FIELD_B);
        CompositeFieldIngestHelper helper = new CompositeFieldIngestHelper(type());
        helper.setup(config);

        Collection<NormalizedContentInterface> values = helper.getCompositeFields(fields(visA, visB)).get(COMPOSITE);
        assertEquals(1, values.size());
        return Iterables.getOnlyElement(values).getVisibility();
    }

    private static ColumnVisibility virtualVisibility(ColumnVisibility visA, ColumnVisibility visB) {
        Configuration config = new Configuration();
        config.set(TYPE_NAME + VirtualIngest.VIRTUAL_FIELD_NAMES, VIRTUAL);
        config.set(TYPE_NAME + VirtualIngest.VIRTUAL_FIELD_MEMBERS, FIELD_A + "." + FIELD_B);
        VirtualFieldIngestHelper helper = new VirtualFieldIngestHelper(type());
        helper.setup(config);

        Collection<NormalizedContentInterface> values = helper.getVirtualFields(fields(visA, visB)).get(VIRTUAL);
        assertEquals(1, values.size());
        return Iterables.getOnlyElement(values).getVisibility();
    }

    @Test
    public void compositeWithoutFieldVisibilitiesUsesEventVisibility() {
        assertNull(compositeVisibility(null, null));
    }

    @Test
    public void compositeWithOneFieldVisibilityKeepsIt() {
        assertEquals(new ColumnVisibility("B&A"), compositeVisibility(new ColumnVisibility("B&A"), null));
        assertEquals(new ColumnVisibility("C|D"), compositeVisibility(null, new ColumnVisibility("C|D")));
    }

    @Test
    public void compositeCombinesFieldVisibilities() {
        assertEquals(new ColumnVisibility("A&B"), compositeVisibility(new ColumnVisibility("A"), new ColumnVisibility("B")));
        assertEquals(new ColumnVisibility("A&B&(C|D)"), compositeVisibility(new ColumnVisibility("B&A"), new ColumnVisibility("D|C")));
        // identical visibilities collapse to a single, normalized expression
        assertEquals(new ColumnVisibility("A&B"), compositeVisibility(new ColumnVisibility("B&A"), new ColumnVisibility("B&A")));
    }

    @Test
    public void compositeIgnoresEmptyFieldVisibility() {
        assertEquals(new ColumnVisibility("A"), compositeVisibility(new ColumnVisibility("A"), new ColumnVisibility()));
    }

    @Test
    public void virtualWithoutFieldVisibilitiesUsesEventVisibility() {
        assertNull(virtualVisibility(null, null));
    }

    @Test
    public void virtualWithOneFieldVisibilityKeepsIt() {
        assertEquals(new ColumnVisibility("B&A"), virtualVisibility(new ColumnVisibility("B&A"), null));
        assertEquals(new ColumnVisibility("C|D"), virtualVisibility(null, new ColumnVisibility("C|D")));
    }

    @Test
    public void virtualCombinesFieldVisibilities() {
        assertEquals(new ColumnVisibility("A&B"), virtualVisibility(new ColumnVisibility("A"), new ColumnVisibility("B")));
        assertEquals(new ColumnVisibility("A&B&(C|D)"), virtualVisibility(new ColumnVisibility("B&A"), new ColumnVisibility("D|C")));
        assertEquals(new ColumnVisibility("A&B"), virtualVisibility(new ColumnVisibility("B&A"), new ColumnVisibility("B&A")));
    }

    @Test
    public void virtualIgnoresEmptyFieldVisibility() {
        assertEquals(new ColumnVisibility("A"), virtualVisibility(new ColumnVisibility(), new ColumnVisibility("A")));
    }
}

package datawave.ingest.mapreduce.handler.atom;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.log4j.Logger;

import com.google.common.base.Strings;
import com.google.common.collect.Multimap;

import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.Type;
import datawave.ingest.data.TypeRegistry;
import datawave.ingest.data.config.ConfigurationHelper;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.data.config.ingest.IngestHelperInterface;
import datawave.ingest.mapreduce.handler.ExtendedDataTypeHandler;
import datawave.ingest.mapreduce.job.BulkIngestKey;
import datawave.ingest.mapreduce.job.writer.ContextWriter;
import datawave.ingest.metadata.RawRecordMetadata;
import datawave.marking.MarkingFunctions;
import datawave.util.StringUtils;
import datawave.util.TextUtil;

/**
 *
 * Handler that creates entries in a table for use with an Atom service.
 *
 * ROW: category name \0 (Long.MAX_VALUE - timestamp), where category name is a field name COLF: field value \0 uuid COLQ: event visibility string CV: event
 * column visibility timestamp: now
 *
 */
public class AtomDataTypeHandler<KEYIN,KEYOUT,VALUEOUT> implements ExtendedDataTypeHandler<KEYIN,KEYOUT,VALUEOUT> {

    private static final Logger log = Logger.getLogger(AtomDataTypeHandler.class);

    public static final String ATOM_TYPE = "atom";
    public static final String ATOM_TABLE_NAME = ATOM_TYPE + ".table.name";
    public static final String ATOM_TABLE_LOADER_PRIORITY = ATOM_TYPE + ".loader.priority";
    public static final String ATOM_TYPES_TO_PROCESS = ATOM_TYPE + ".configured.types";
    public static final String ATOM_FIELD_NAMES = ATOM_TYPE + ".category.field.names";
    public static final String ATOM_FIELD_ALIASES = ATOM_TYPE + ".category.field.names.aliases";
    public static final String ATOM_FIELD_VALUE_OVERRIDES = ATOM_TYPE + ".category.field.value.overrides";
    public static final String ATOM_CATEGORY_SUB_FIELD = ATOM_TYPE + ".category.field.value.sub";

    protected static final Value NULL_VALUE = new Value(new byte[0]);

    protected String tableName = null;
    protected String categoryTableName = null;
    protected String[] fieldNames = null;
    protected String[] fieldAliases = null;
    protected String[] fieldOverrides = null;
    protected HashMap<String,Set<String>> subCategories;
    protected String[] sCategories = null;
    protected MarkingFunctions markingFunctions;

    protected Configuration conf;

    @Override
    public void setup(TaskAttemptContext context) {
        conf = context.getConfiguration();
        tableName = ConfigurationHelper.isNull(context.getConfiguration(), ATOM_TABLE_NAME, String.class);
        categoryTableName = tableName + "Categories";
        subCategories = new HashMap<>();
        markingFunctions = MarkingFunctions.Factory.createMarkingFunctions();

        TypeRegistry.getInstance(context.getConfiguration());
        String[] types = ConfigurationHelper.isNull(context.getConfiguration(), ATOM_TYPES_TO_PROCESS, String[].class);
        // Set up the ingest helpers for the known datatypes.

        fieldNames = ConfigurationHelper.isNull(context.getConfiguration(), ATOM_FIELD_NAMES, String[].class);
        // Configuration.getStrings() eats empty values, we don't want to do that. Split it ourselves.
        String aliases = ConfigurationHelper.isNull(context.getConfiguration(), ATOM_FIELD_ALIASES, String.class);
        fieldAliases = StringUtils.split(aliases, ',', true); // keeps empty elements
        String overrides = ConfigurationHelper.isNull(context.getConfiguration(), ATOM_FIELD_VALUE_OVERRIDES, String.class);
        fieldOverrides = StringUtils.split(overrides, ',', true); // keeps empty elements

        sCategories = StringUtils.split(ConfigurationHelper.isNull(context.getConfiguration(), ATOM_CATEGORY_SUB_FIELD, String.class), ',', false);

        Set<String> tSet;
        for (String s : sCategories) {
            String field_value[] = StringUtils.split(s, ':', false);
            if (field_value.length == 2 && (!Strings.isNullOrEmpty(field_value[0]) && !Strings.isNullOrEmpty(field_value[1]))) {

                if (!subCategories.containsKey(field_value[0])) {

                    tSet = new HashSet<>();

                } else {

                    tSet = subCategories.get(field_value[0]);

                }

                System.err.println("Value: " + field_value[0] + " " + field_value[1]);
                tSet.add(field_value[1]);
                subCategories.put(field_value[0], tSet);

            }

        }

        // Make sure these 3 arrays are all the same size.
        if (fieldNames.length != fieldAliases.length && fieldNames.length != fieldOverrides.length) {
            throw new IllegalArgumentException("AtomDataTypeHandler, configured fieldNames, fieldAliases, and fieldOverrides are different lengtsh.  "
                            + "Please fix the configuration. " + fieldNames.length + "," + fieldAliases.length + "," + fieldOverrides.length);
        }
    }

    @Override
    public String[] getTableNames(Configuration conf) {
        // Setup is not called before this method is invoked in IngestJob.
        String a = ConfigurationHelper.isNull(conf, ATOM_TABLE_NAME, String.class);
        String c = a + "Categories";
        return new String[] {a, c};
    }

    @Override
    public int[] getTableLoaderPriorities(Configuration conf) {
        int priority = ConfigurationHelper.isNull(conf, ATOM_TABLE_LOADER_PRIORITY, Integer.class);
        return new int[] {priority, priority};
    }

    @Override
    public Multimap<BulkIngestKey,Value> processBulk(KEYIN key, RawRecordContainer event, Multimap<String,NormalizedContentInterface> fields,
                    StatusReporter reporter) {
        throw new UnsupportedOperationException("processBulk is not supported, please use process");
    }

    @Override
    public IngestHelperInterface getHelper(Type datatype) {
        return datatype.getIngestHelper(this.conf);
    }

    @Override
    public void close(TaskAttemptContext context) {}

    @Override
    public RawRecordMetadata getMetadata() {
        return null;
    }

    @Override
    public long process(KEYIN key, RawRecordContainer event, Multimap<String,NormalizedContentInterface> fields,
                    TaskInputOutputContext<KEYIN,? extends RawRecordContainer,KEYOUT,VALUEOUT> context, ContextWriter<KEYOUT,VALUEOUT> contextWriter)
                    throws IOException, InterruptedException {
        int count = 0;

        if (getHelper(event.getDataType()) == null) {
            return count;
        }

        Text tname = new Text(tableName);
        Set<Key> categories = new HashSet<>();

        // Get the list of fields names for this datatype that we want to create alerts for.
        for (int i = 0; i < this.fieldNames.length; i++) {

            // If no field with this name, abort
            if (!fields.containsKey(this.fieldNames[i]))
                continue;

            String atomFieldName = this.fieldNames[i];
            String keyFieldName = atomFieldName;
            // Check for field alias
            if (!("".equals(this.fieldAliases[i])))
                keyFieldName = this.fieldAliases[i];

            String id = event.getAltIds().iterator().next();
            if ("".equals(this.fieldOverrides[i])) {
                for (NormalizedContentInterface nci : fields.get(atomFieldName)) {
                    String columnQualifier = getColumnQualifier(event, nci);
                    Key k = createKey(keyFieldName, nci.getEventFieldValue(), columnQualifier, id, event.getVisibility(), event.getTimestamp());
                    BulkIngestKey bk = new BulkIngestKey(tname, k);
                    contextWriter.write(bk, NULL_VALUE, context);
                    count++;
                    Key categoryKey = new Key(keyFieldName, "", "", event.getVisibility(), event.getTimestamp());
                    categories.add(categoryKey);
                    if (subCategories.containsKey(atomFieldName)) {
                        if (subCategories.get(atomFieldName).contains(nci.getEventFieldValue())) {
                            Key k2 = createKey(keyFieldName + "/" + nci.getEventFieldValue(), nci.getEventFieldValue(), columnQualifier, id,
                                            event.getVisibility(), event.getTimestamp());
                            BulkIngestKey bk2 = new BulkIngestKey(tname, k2);
                            contextWriter.write(bk2, NULL_VALUE, context);
                            count++;
                            Key categoryKey2 = new Key(keyFieldName + "/" + nci.getEventFieldValue(), "", "", event.getVisibility(), event.getTimestamp());
                            categories.add(categoryKey2);
                        }
                    }
                }
            } else {

                String columnQualifier = getColumnQualifier(event, null);
                // use the override
                Key k = createKey(keyFieldName, this.fieldOverrides[i], columnQualifier, id, event.getVisibility(), event.getTimestamp());
                BulkIngestKey bk = new BulkIngestKey(tname, k);
                contextWriter.write(bk, NULL_VALUE, context);
                count++;
                Key categoryKey = new Key(keyFieldName, "", "", event.getVisibility(), event.getTimestamp());
                categories.add(categoryKey);

                if (subCategories.containsKey(atomFieldName)) {
                    if (subCategories.get(atomFieldName).contains(this.fieldOverrides[i])) {
                        Key k2 = createKey(keyFieldName + "/" + this.fieldOverrides[i], this.fieldOverrides[i], columnQualifier, id, event.getVisibility(),
                                        event.getTimestamp());
                        BulkIngestKey bk2 = new BulkIngestKey(tname, k2);
                        contextWriter.write(bk2, NULL_VALUE, context);
                        count++;
                        Key categoryKey2 = new Key(keyFieldName + "/" + this.fieldOverrides[i], "", "", event.getVisibility(), event.getTimestamp());
                        categories.add(categoryKey2);
                    }
                }
            }
        }

        Text categoryTableName = new Text(this.categoryTableName);
        for (Key catKey : categories) {
            BulkIngestKey bk = new BulkIngestKey(categoryTableName, catKey);
            contextWriter.write(bk, NULL_VALUE, context);
            count++;
        }
        return count;
    }

    public Key createKey(String fieldName, String fieldValue, String columnQualifier, String uuid, ColumnVisibility viz, long timestamp) {

        Text row = new Text(fieldName);
        TextUtil.textAppend(row, (Long.MAX_VALUE - timestamp));

        Text colf = new Text(fieldValue);
        TextUtil.textAppend(colf, uuid);

        return new Key(row, colf, new Text(columnQualifier), viz, System.currentTimeMillis());

    }

    /**
     * A helper routine to determine the visibility for a field.
     *
     * @param event
     *            the event record
     * @param value
     *            the value
     * @return the visibility
     */

    protected String getColumnQualifier(RawRecordContainer event, NormalizedContentInterface value) {
        ColumnVisibility visibility = event.getVisibility();
        if (value.getMarkings() != null && !value.getMarkings().isEmpty()) {
            try {
                visibility = markingFunctions.translateToColumnVisibility(value.getMarkings());
            } catch (MarkingFunctions.Exception e) {
                throw new RuntimeException("Cannot convert record-level markings into a column visibility", e);

            }
        }
        return flatten(visibility);
    }

    /**
     * Create a flattened visibility, using the cache if possible
     *
     * @param vis
     *            the column visibility
     * @return the flattened visibility
     */
    protected String flatten(ColumnVisibility vis) {
        return new String(markingFunctions.flatten(vis));
    }

}

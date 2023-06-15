package datawave.ingest.json.config.helper;

import datawave.ingest.data.config.CSVHelper;
import datawave.ingest.json.util.JsonObjectFlattener;
import datawave.ingest.json.util.JsonObjectFlattener.FlattenMode;
import org.apache.hadoop.conf.Configuration;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * <p>
 * DataTypeHelper for json data. Extends CSVHelper to enable "header" field and "extra" field configuration options, whitelist/blacklist options, and many
 * others, most of which can be used to affect behavior of json parsing, if needed
 */
public class JsonDataTypeHelper extends CSVHelper {

    public interface Properties extends CSVHelper.Properties {

        String COLUMN_VISIBILITY_FIELD = ".data.category.marking.visibility.field";
        String FLATTENER_MODE = ".data.json.flattener.mode";

    }

    protected String columnVisibilityField = null;
    protected FlattenMode jsonObjectFlattenMode = FlattenMode.NORMAL;

    @Override
    public void setup(Configuration config) throws IllegalArgumentException {
        super.setup(config);
        this.setJsonObjectFlattenModeByName(config.get(this.getType().typeName() + Properties.FLATTENER_MODE, FlattenMode.NORMAL.name()));
        this.setColumnVisibilityField(config.get(this.getType().typeName() + Properties.COLUMN_VISIBILITY_FIELD));
    }

    public String getColumnVisibilityField() {
        return columnVisibilityField;
    }

    public void setColumnVisibilityField(String columnVisibilityField) {
        this.columnVisibilityField = columnVisibilityField;
    }

    public FlattenMode getJsonObjectFlattenMode() {
        return this.jsonObjectFlattenMode;
    }

    public void setJsonObjectFlattenModeByName(String jsonObjectFlattenMode) {
        this.jsonObjectFlattenMode = FlattenMode.valueOf(jsonObjectFlattenMode);
    }

    public void setJsonObjectFlattenMode(FlattenMode mode) {
        this.jsonObjectFlattenMode = mode;
    }

    public JsonObjectFlattener newFlattener() {

        // Set flattener's whitelist and blacklist according to current state of the helper

        Set<String> whitelistFields;
        Set<String> blacklistFields;

        if (this.getHeader() != null && this.getHeader().length > 0 && !this.processExtraFields()) {
            // In this case, 'header' fields are enabled and the client doesn't want to process any non-header
            // fields. This forces our whitelist to include the header fields themselves...
            whitelistFields = new HashSet<>(Arrays.asList(this.getHeader()));
            // Add to that any fields explicitly configured to be whitelisted
            if (null != this.getFieldWhitelist()) {
                whitelistFields.addAll(this.getFieldWhitelist());
            }
        } else if (null != this.getFieldWhitelist()) {
            whitelistFields = this.getFieldWhitelist();
        } else {
            whitelistFields = Collections.EMPTY_SET;
        }

        if (null != this.getFieldBlacklist()) {
            blacklistFields = this.getFieldBlacklist();
        } else {
            blacklistFields = Collections.EMPTY_SET;
        }

        return new JsonIngestFlattener.Builder().jsonDataTypeHelper(this).mapKeyWhitelist(whitelistFields).mapKeyBlacklist(blacklistFields)
                        .flattenMode(getJsonObjectFlattenMode()).addArrayIndexToFieldName(false).build();
    }
}

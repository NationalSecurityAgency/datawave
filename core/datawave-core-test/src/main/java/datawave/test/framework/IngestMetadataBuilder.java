package datawave.test.framework;

import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Preconditions;

import datawave.data.type.Type;
import datawave.test.framework.util.MetadataColumn;

/**
 * A builder for {@link IngestMetadata}
 */
public class IngestMetadataBuilder {

    private boolean alphabeticFieldsEnabled = false;
    private boolean numericFieldsEnabled = false;
    private int numShards = IngestMetadata.DEFAULT_NUM_SHARDS;
    private final List<MetadataColumn> metadataColumns = new ArrayList<>();
    private final List<Type<?>> normalizers = new ArrayList<>();

    private IngestMetadataBuilder() {
        // enforce static access
    }

    public static IngestMetadataBuilder builder() {
        return new IngestMetadataBuilder();
    }

    public IngestMetadataBuilder setMetadataColumns(List<MetadataColumn> metadataColumns) {
        Preconditions.checkNotNull(metadataColumns, "metadataColumns cannot be null");
        Preconditions.checkState(!metadataColumns.isEmpty(), "metadataColumns cannot be empty");
        this.metadataColumns.addAll(metadataColumns);
        return this;
    }

    public IngestMetadataBuilder addNormalizer(Type<?> normalizer) {
        Preconditions.checkNotNull(normalizer, "normalizer cannot be null");
        this.normalizers.add(normalizer);
        return this;
    }

    public IngestMetadataBuilder addNormalizers(List<Type<?>> normalizers) {
        Preconditions.checkNotNull(normalizers, "normalizers cannot be null");
        Preconditions.checkState(!normalizers.isEmpty(), "normalizers cannot be empty");
        this.normalizers.addAll(normalizers);
        return this;
    }

    public IngestMetadataBuilder enableAlphabeticFields() {
        this.alphabeticFieldsEnabled = true;
        return this;
    }

    public IngestMetadataBuilder enableNumericFields() {
        this.numericFieldsEnabled = true;
        return this;
    }

    public IngestMetadataBuilder setNumShards(int numShards) {
        Preconditions.checkArgument(numShards > 0, "numShards must be greater than 0");
        this.numShards = numShards;
        return this;
    }

    public IngestMetadata build() {
        Preconditions.checkState(!metadataColumns.isEmpty(), "metadataColumns must be set");
        Preconditions.checkState(!normalizers.isEmpty(), "normalizers must be set");
        Preconditions.checkState(alphabeticFieldsEnabled || numericFieldsEnabled, "alphabetic or numeric fields must be enabled");
        return new IngestMetadata(metadataColumns, normalizers, alphabeticFieldsEnabled, numericFieldsEnabled, numShards);
    }
}

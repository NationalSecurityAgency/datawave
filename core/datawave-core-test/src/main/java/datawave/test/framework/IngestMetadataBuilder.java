package datawave.test.framework;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Preconditions;

import datawave.data.type.Type;
import datawave.test.framework.util.MetadataColumn;

/**
 * A builder for {@link IngestMetadata}. Single-use: {@link #build()} may be called once.
 */
public class IngestMetadataBuilder {

    // safe to share, as SecureRandom is thread-safe, and cheaper than a per-builder instance re-seeding from the entropy source on every build
    private static final SecureRandom SEED_GENERATOR = new SecureRandom();

    private boolean alphabeticFieldsEnabled = false;
    private boolean numericFieldsEnabled = false;
    private int numShards = IngestMetadata.DEFAULT_NUM_SHARDS;
    private final List<MetadataColumn> metadataColumns = new ArrayList<>();
    private final List<Type<?>> normalizers = new ArrayList<>();

    // a fresh seed per build keeps the suite exploring new data; IngestMetadata logs it so any run can be replayed via setSeed
    private long seed = SEED_GENERATOR.nextLong();

    private boolean built = false;

    private IngestMetadataBuilder() {
        // enforce static access
    }

    public static IngestMetadataBuilder builder() {
        return new IngestMetadataBuilder();
    }

    /**
     * Set the metadata columns, replacing any set previously.
     *
     * @param metadataColumns
     *            the metadata columns
     * @return this builder
     */
    public IngestMetadataBuilder setMetadataColumns(List<MetadataColumn> metadataColumns) {
        Preconditions.checkNotNull(metadataColumns, "metadataColumns cannot be null");
        Preconditions.checkArgument(!metadataColumns.isEmpty(), "metadataColumns cannot be empty");
        this.metadataColumns.clear();
        this.metadataColumns.addAll(metadataColumns);
        return this;
    }

    /**
     * Add a normalizer to those already configured.
     *
     * @param normalizer
     *            the normalizer
     * @return this builder
     */
    public IngestMetadataBuilder addNormalizer(Type<?> normalizer) {
        Preconditions.checkNotNull(normalizer, "normalizer cannot be null");
        Preconditions.checkArgument(IngestMetadata.isSupportedNormalizer(normalizer), "normalizer not supported: %s", normalizer.getClass().getName());
        this.normalizers.add(normalizer);
        return this;
    }

    /**
     * Add normalizers to those already configured.
     *
     * @param normalizers
     *            the normalizers
     * @return this builder
     */
    public IngestMetadataBuilder addNormalizers(List<Type<?>> normalizers) {
        Preconditions.checkNotNull(normalizers, "normalizers cannot be null");
        Preconditions.checkArgument(!normalizers.isEmpty(), "normalizers cannot be empty");
        normalizers.forEach(this::addNormalizer);
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

    /**
     * Pin the seed every generated value derives from.
     * <p>
     * Left unset, each build picks a fresh seed so the suite keeps exploring new data. {@link IngestMetadata#createEvents(int, int)} logs whichever seed is in
     * effect, so a failing run can be replayed exactly by passing that value here.
     *
     * @param seed
     *            the seed
     * @return this builder
     */
    public IngestMetadataBuilder setSeed(long seed) {
        this.seed = seed;
        return this;
    }

    /**
     * Build the ingest metadata, rejecting a configuration that would generate no fields.
     * <p>
     * Reuse is rejected rather than supported: the seed is drawn once per builder, so a second build would quietly repeat the first instance's data unless
     * {@link #setSeed(long)} intervened.
     *
     * @return the configured ingest metadata
     */
    public IngestMetadata build() {
        Preconditions.checkState(!built, "build() has already been called on this builder");
        Preconditions.checkState(!metadataColumns.isEmpty(), "metadataColumns must be set");
        Preconditions.checkState(!normalizers.isEmpty(), "normalizers must be set");
        Preconditions.checkState(alphabeticFieldsEnabled || numericFieldsEnabled, "alphabetic or numeric fields must be enabled");
        IngestMetadata metadata = new IngestMetadata(metadataColumns, normalizers, alphabeticFieldsEnabled, numericFieldsEnabled, numShards, seed);
        built = true;
        return metadata;
    }
}

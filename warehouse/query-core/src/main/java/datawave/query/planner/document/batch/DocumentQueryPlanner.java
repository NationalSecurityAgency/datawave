package datawave.query.planner.document.batch;

import com.google.common.base.Joiner;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import datawave.data.type.Type;
import datawave.query.config.DocumentQueryConfiguration;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.exceptions.DatawaveQueryException;
import datawave.query.iterator.QueryOptions;
import datawave.query.planner.DefaultQueryPlanner;
import datawave.query.util.MetadataHelper;
import datawave.query.util.TypeMetadata;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.QueryException;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.TableNotFoundException;

import java.io.IOException;

/**
 * Extends DefaultQueryPlanner to support enforcing what types can be used, downselected from a list provided at
 * configuration.
 *
 */
public class DocumentQueryPlanner extends DefaultQueryPlanner implements Cloneable {

    public DocumentQueryPlanner() {
        this(Long.MAX_VALUE);
    }

    public DocumentQueryPlanner(long _maxRangesPerQueryPiece) {
        this(_maxRangesPerQueryPiece, true);
    }

    public DocumentQueryPlanner(long maxRangesPerQueryPiece, boolean limitScanners) {
        super(maxRangesPerQueryPiece,limitScanners);
    }

    protected DocumentQueryPlanner(DocumentQueryPlanner other) {
        super(other);
    }
    @Override
    protected void configureIterator(ShardQueryConfiguration config, IteratorSetting cfg, String newQueryString, boolean isFullTable)
            throws DatawaveQueryException {

        // Load enrichers, filters, unevaluatedExpressions, and projection
        // fields
        setCommonIteratorOptions(config, cfg);

        addOption(cfg, QueryOptions.LIMIT_FIELDS, config.getLimitFieldsAsString(), true);
        addOption(cfg, QueryOptions.GROUP_FIELDS, config.getGroupFieldsAsString(), true);
        addOption(cfg, QueryOptions.GROUP_FIELDS_BATCH_SIZE, config.getGroupFieldsBatchSizeAsString(), true);
        addOption(cfg, QueryOptions.UNIQUE_FIELDS, config.getUniqueFields().toString(), true);
        addOption(cfg, QueryOptions.EXCERPT_FIELDS, config.getExcerptFields().toString(), true);
        addOption(cfg, QueryOptions.EXCERPT_ITERATOR, config.getExcerptIterator().getName(), false);
        addOption(cfg, QueryOptions.HIT_LIST, Boolean.toString(config.isHitList()), false);
        addOption(cfg, QueryOptions.TERM_FREQUENCY_FIELDS, Joiner.on(',').join(config.getQueryTermFrequencyFields()), false);
        addOption(cfg, QueryOptions.TERM_FREQUENCIES_REQUIRED, Boolean.toString(config.isTermFrequenciesRequired()), true);
        addOption(cfg, QueryOptions.QUERY, newQueryString, false);
        addOption(cfg, QueryOptions.QUERY_ID, config.getQuery().getId().toString(), false);
        addOption(cfg, QueryOptions.FULL_TABLE_SCAN_ONLY, Boolean.toString(isFullTable), false);
        addOption(cfg, QueryOptions.TRACK_SIZES, Boolean.toString(config.isTrackSizes()), true);
        addOption(cfg, QueryOptions.ACTIVE_QUERY_LOG_NAME, config.getActiveQueryLogName(), true);
        // Set the start and end dates
        if (config instanceof DocumentQueryConfiguration) {
            DocumentQueryConfiguration docConfig = DocumentQueryConfiguration.class.cast(config);
            configureTypeMappings(docConfig, cfg, metadataHelper, compressMappings, docConfig.getForceAllTypes());
        }
        else{
            configureTypeMappings(config, cfg, metadataHelper, compressMappings);
        }
    }


    public static void configureTypeMappings(DocumentQueryConfiguration config, IteratorSetting cfg, MetadataHelper metadataHelper, boolean compressMappings,  boolean forceAllTypes)
            throws DatawaveQueryException {
        try {
            addOption(cfg, QueryOptions.QUERY_MAPPING_COMPRESS, Boolean.valueOf(compressMappings).toString(), false);

            // now lets filter the query field datatypes to those that are not
            // indexed
            Multimap<String,Type<?>> nonIndexedQueryFieldsDatatypes = HashMultimap.create(config.getQueryFieldsDatatypes());
            nonIndexedQueryFieldsDatatypes.keySet().removeAll(config.getIndexedFields());

            TypeMetadata metadata = metadataHelper.getTypeMetadata(config.getDatatypeFilter());

            String nonIndexedTypes = QueryOptions.buildFieldNormalizerString(nonIndexedQueryFieldsDatatypes);
            String typeMetadataString = forceAllTypes ? forceTypes(config.getAllowedTypes(),metadata) : metadata.toString();
            String requiredAuthsString = metadataHelper.getUsersMetadataAuthorizationSubset();

            if (compressMappings) {
                nonIndexedTypes = QueryOptions.compressOption(nonIndexedTypes, QueryOptions.UTF8);
                typeMetadataString = QueryOptions.compressOption(typeMetadataString, QueryOptions.UTF8);
                requiredAuthsString = QueryOptions.compressOption(requiredAuthsString, QueryOptions.UTF8);
            }
            addOption(cfg, QueryOptions.NON_INDEXED_DATATYPES, nonIndexedTypes, false);
            addOption(cfg, QueryOptions.TYPE_METADATA, typeMetadataString, false);
            addOption(cfg, QueryOptions.TYPE_METADATA_AUTHS, requiredAuthsString, false);
            addOption(cfg, QueryOptions.METADATA_TABLE_NAME, config.getMetadataTableName(), false);

        } catch (TableNotFoundException | IOException e) {
            QueryException qe = new QueryException(DatawaveErrorCode.TYPE_MAPPING_CONFIG_ERROR, e);
            throw new DatawaveQueryException(qe);
        }

    }
}

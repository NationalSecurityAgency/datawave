package datawave.query.testframework;

import static datawave.query.testframework.FileType.CSV;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import org.junit.Assert;

import datawave.data.type.LcNoDiacriticsType;
import datawave.ingest.csv.config.helper.ExtendedCSVHelper;
import datawave.ingest.csv.config.helper.ExtendedCSVIngestHelper;
import datawave.ingest.csv.mr.handler.ContentCSVColumnBasedHandler;
import datawave.ingest.csv.mr.input.CSVRecordReader;
import datawave.ingest.data.TypeRegistry;
import datawave.ingest.data.config.CSVHelper;
import datawave.ingest.data.config.DataTypeHelper;
import datawave.ingest.data.config.MarkingsHelper;
import datawave.ingest.data.config.ingest.BaseIngestHelper;
import datawave.ingest.data.config.ingest.CompositeIngest;
import datawave.ingest.data.config.ingest.ContentBaseIngestHelper;
import datawave.ingest.data.config.ingest.VirtualIngest;
import datawave.ingest.json.config.helper.JsonIngestHelper;
import datawave.ingest.json.mr.handler.ContentJsonColumnBasedHandler;
import datawave.ingest.json.mr.input.JsonRecordReader;
import datawave.ingest.mapreduce.handler.shard.AbstractColumnBasedHandler;
import datawave.ingest.mapreduce.handler.shard.ShardedDataTypeHandler;
import datawave.ingest.mapreduce.handler.tokenize.ExtendedContentDataTypeHelper;
import datawave.policy.IngestPolicyEnforcer;

/**
 * Abstract base class that contains the configuration settings for test data types.
 */
public abstract class AbstractDataTypeConfig implements DataTypeHadoopConfig {

    private static final Logger log = LogManager.getLogger(AbstractDataTypeConfig.class);

    // ===============================================
    // common constants for all data types
    protected static final String DATE_FIELD_FORMAT = "yyyyMMdd";
    public static final SimpleDateFormat YMD_DateFormat = new SimpleDateFormat(DATE_FIELD_FORMAT);
    private static final String TEST_VISIBILITY = "public";
    private static final String[] AUTH_VALUES = new String[] {"public", "private", "Euro", "NA"};
    private static final Authorizations TEST_AUTHS = new Authorizations(AUTH_VALUES);

    /**
     * Retrieves an {@link Authorizations} object to use for query.
     *
     * @return valid auths object
     */
    public static Authorizations getTestAuths() {
        return TEST_AUTHS;
    }

    /**
     * Returns the default visibility applied to all datatypes.
     *
     * @return default column visibility
     */
    public static ColumnVisibility getDefaultVisibility() {
        return new ColumnVisibility(TEST_VISIBILITY);
    }

    private static List<String> mappingToNames(final Collection<Set<String>> mapping) {
        final List<String> names = new ArrayList<>();
        for (final Set<String> composite : mapping) {
            names.add(String.join("_", composite));
        }

        return names;
    }

    private static List<String> mappingToFields(final Collection<Set<String>> mapping) {
        final List<String> fields = new ArrayList<>();
        for (final Set<String> virtual : mapping) {
            fields.add(String.join(".", virtual));
        }
        return fields;
    }

    // ===============================================
    // inherited instance members
    protected final String dataType;
    protected final URI ingestPath;
    /**
     * Includes on the fields that are indexed (none of the composite fields).
     */
    private final FieldConfig fieldConfig;
    protected final Configuration hConf = new Configuration();

    /**
     * @param dt
     *            datatype name
     * @param ingestFile
     *            ingest file name
     * @param config
     *            field config for accumulo
     * @param manager
     *            data manager for data
     * @throws IOException
     *             unable to load ingest file
     * @throws URISyntaxException
     *             ingest file uri conversion error
     */
    protected AbstractDataTypeConfig(final String dt, final String ingestFile, final FieldConfig config, final RawDataManager manager)
                    throws IOException, URISyntaxException {
        this(dt, ingestFile, CSV, config, manager);
    }

    /**
     * @param dt
     *            datatype name
     * @param ingestFile
     *            ingest file name
     * @param config
     *            field config for accumulo
     * @param format
     *            file type containing test data
     * @param manager
     *            data manager for data
     * @throws IOException
     *             unable to load ingest file
     * @throws URISyntaxException
     *             ingest file uri conversion error
     */
    protected AbstractDataTypeConfig(final String dt, final String ingestFile, final FileType format, final FieldConfig config, final RawDataManager manager)
                    throws IOException, URISyntaxException {
        log.info("---------  loading datatype (" + dt + ") ingest file(" + ingestFile + ") ---------");

        // RawDataManager manager = mgr;
        URL url = this.getClass().getClassLoader().getResource(ingestFile);
        Assert.assertNotNull("unable to resolve ingest file(" + ingestFile + ")", url);
        // if this url is not a file (e.g. nested in a jar), then make it a file
        if (url.getProtocol().startsWith("jar")) {
            InputStream in = this.getClass().getClassLoader().getResourceAsStream(ingestFile);
            File tempFile = File.createTempFile(String.valueOf(in.hashCode()), ".tmp");
            tempFile.deleteOnExit();
            FileUtils.copyInputStreamToFile(in, tempFile);
            url = tempFile.toURI().toURL();
        }
        this.ingestPath = url.toURI();
        this.dataType = dt;
        this.fieldConfig = config;

        // default Hadoop settings - override if needed
        this.hConf.set(DataTypeHelper.Properties.DATA_NAME, this.dataType);

        switch (format) {
            case CSV:
                // load raw data into POJO for CSV
                Set<String> anyFieldIndexes = new HashSet<>(this.fieldConfig.getIndexFields());
                anyFieldIndexes.addAll(this.fieldConfig.getReverseIndexFields());
                manager.addTestData(this.ingestPath, this.dataType, anyFieldIndexes);
                this.hConf.set(this.dataType + TypeRegistry.INGEST_HELPER, ExtendedTestCSVIngestHelper.class.getName());
                this.hConf.set(this.dataType + TypeRegistry.RAW_READER, CSVRecordReader.class.getName());

                this.hConf.set(this.dataType + ExtendedCSVHelper.Properties.EVENT_SECURITY_MARKING_FIELD_NAMES, getSecurityMarkingFieldNames());
                this.hConf.set(this.dataType + ExtendedCSVHelper.Properties.EVENT_SECURITY_MARKING_FIELD_DOMAINS, getSecurityMarkingFieldDomains());
                this.hConf.set(this.dataType + TypeRegistry.HANDLER_CLASSES, ContentCSVColumnBasedHandler.class.getName());
                break;
            case JSON:
                // loading of raw data for JSON is postponed until the Accumulo configuration is complete
                this.hConf.set(this.dataType + TypeRegistry.INGEST_HELPER, JsonIngestHelper.class.getName());
                this.hConf.set(this.dataType + TypeRegistry.RAW_READER, JsonRecordReader.class.getName());
                this.hConf.set(this.dataType + TypeRegistry.HANDLER_CLASSES, ContentJsonColumnBasedHandler.class.getName());
                break;
            case GROUPING:
                // nothing to do here
                this.hConf.set(this.dataType + TypeRegistry.HANDLER_CLASSES, AbstractColumnBasedHandler.class.getName());
                break;
            default:
                throw new AssertionError("unhandled format type: " + format.name());
        }

        this.hConf.set(this.dataType + ContentBaseIngestHelper.TOKEN_FIELDNAME_DESIGNATOR_ENABLED, "false");
        this.hConf.set(this.dataType + BaseIngestHelper.DEFAULT_TYPE, LcNoDiacriticsType.class.getName());
        this.hConf.set(this.dataType + CSVHelper.DATA_SEP, ",");
        this.hConf.set(this.dataType + CSVHelper.MULTI_VALUED_SEPARATOR, RawDataManager.MULTIVALUE_SEP);

        this.hConf.set(this.dataType + DataTypeHelper.Properties.INGEST_POLICY_ENFORCER_CLASS, IngestPolicyEnforcer.NoOpIngestPolicyEnforcer.class.getName());

        this.hConf.set(this.dataType + ".data.auth.id.mode", "NEVER");
        this.hConf.set(ShardedDataTypeHandler.METADATA_TERM_FREQUENCY, "true");

        this.hConf.set(this.dataType + MarkingsHelper.DEFAULT_MARKING, new String(getDefaultVisibility().getExpression(), StandardCharsets.UTF_8));

        // virtual field separator
        this.hConf.set(this.dataType + VirtualIngest.VIRTUAL_FIELD_VALUE_SEPARATOR, "|");

        // index fields also include composite fields
        Set<String> indexEntries = new HashSet<>(this.fieldConfig.getIndexFields());
        List<String> compositeNames = mappingToNames(this.fieldConfig.getCompositeFields());
        indexEntries.addAll(compositeNames);
        this.hConf.set(this.dataType + BaseIngestHelper.INDEX_FIELDS, String.join(",", indexEntries));
        this.hConf.set(this.dataType + BaseIngestHelper.INDEX_ONLY_FIELDS, String.join(",", this.fieldConfig.getIndexOnlyFields()));
        this.hConf.set(this.dataType + BaseIngestHelper.REVERSE_INDEX_FIELDS, String.join(",", this.fieldConfig.getReverseIndexFields()));

        // take the intersection of the tokenized fields with the indexed fields to determine what to tokenize
        Set<String> fields = new HashSet<>(this.fieldConfig.getIndexFields());
        fields.retainAll(this.fieldConfig.getTokenizedFields());
        this.hConf.set(this.dataType + ContentBaseIngestHelper.TOKEN_INDEX_ALLOWLIST, String.join(",", fields));
        fields.clear();
        fields.addAll(this.fieldConfig.getReverseIndexFields());
        fields.retainAll(this.fieldConfig.getTokenizedFields());
        this.hConf.set(this.dataType + ContentBaseIngestHelper.TOKEN_REV_INDEX_ALLOWLIST, String.join(",", fields));

        // setup composite field mappings
        Iterator<Set<String>> componentFieldsIter = this.fieldConfig.getCompositeFields().iterator();
        for (final String composite : compositeNames) {
            this.hConf.set(this.dataType + "." + composite + CompositeIngest.COMPOSITE_FIELD_MAP, String.join(",", componentFieldsIter.next()));
        }

        // virtual fields
        this.hConf.set(this.dataType + VirtualIngest.VIRTUAL_FIELD_NAMES, String.join(",", mappingToNames(this.fieldConfig.getVirtualFields())));
        this.hConf.set(this.dataType + VirtualIngest.VIRTUAL_FIELD_MEMBERS, String.join(",", mappingToFields(this.fieldConfig.getVirtualFields())));

        // multivalue fields - only set if there are values
        Set<String> multi = this.fieldConfig.getMultiValueFields();
        if (!multi.isEmpty()) {
            this.hConf.set(this.dataType + CSVHelper.MULTI_VALUED_FIELDS, String.join(",", multi));
        }
    }

    public String getSecurityMarkingFieldNames() {
        return "";
    }

    public String getSecurityMarkingFieldDomains() {
        return "";
    }

    /**
     * Returns the default list of shards from the class {@link BaseShardIdRange}. Test data types that use a different set of shard ids should override this
     * method.
     *
     * @return list of shard id values
     */
    @Override
    public Collection<String> getShardIds() {
        return BaseShardIdRange.getShardDates();
    }

    @Override
    public Configuration getHadoopConfiguration() {
        return this.hConf;
    }

    @Override
    public String dataType() {
        return this.dataType;
    }

    @Override
    public URI getIngestFile() {
        return this.ingestPath;
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + "{" + "dataType='" + dataType + '\'' + ", ingestPath=" + ingestPath + ", fieldConfig=" + fieldConfig + '}';
    }
}

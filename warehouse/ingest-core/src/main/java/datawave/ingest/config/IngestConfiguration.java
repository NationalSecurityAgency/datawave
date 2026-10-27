package datawave.ingest.config;

import static datawave.ingest.data.config.FieldConfigHelperConstants.FIELD_CONFIG_FILE;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;

import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.Type;
import datawave.ingest.data.config.FieldConfigHelper;
import datawave.ingest.data.config.MarkingsHelper;
import datawave.ingest.data.config.MaskedFieldHelper;
import datawave.ingest.data.config.XMLFieldConfigHelper;
import datawave.ingest.data.config.ingest.BaseIngestHelper;
import datawave.ingest.metadata.RawRecordMetadata;

public interface IngestConfiguration {

    default FieldConfigHelper getFieldConfigHelper(Configuration config, Type dataType, BaseIngestHelper helper) {
        String typeName = dataType.typeName();
        String fieldConfigFile = config.get(typeName + FIELD_CONFIG_FILE);
        if (fieldConfigFile == null) {
            return null;
        }
        return XMLFieldConfigHelper.load(fieldConfigFile, helper, config);
    }

    MarkingsHelper getMarkingsHelper(Configuration config, Type dataType);

    MaskedFieldHelper createMaskedFieldHelper();

    RawRecordContainer createRawRecordContainer();

    RawRecordMetadata createMetadata(Text shardTableName, Text metadataTableName, Text loadDatesTableName, Text shardIndexTableName,
                    Text shardReverseIndexTableName, boolean frequency);

    MimeDecoder createMimeDecoder();
}

package datawave.ingest.config;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;

import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.Type;
import datawave.ingest.data.config.MarkingsHelper;
import datawave.ingest.data.config.MaskedFieldHelper;
import datawave.ingest.metadata.RawRecordMetadata;

public interface IngestConfiguration {

    MarkingsHelper getMarkingsHelper(Configuration config, Type dataType);

    MaskedFieldHelper createMaskedFieldHelper();

    RawRecordContainer createRawRecordContainer();

    RawRecordMetadata createMetadata(Text shardTableName, Text metadataTableName, Text loadDatesTableName, Text shardIndexTableName,
                    Text shardReverseIndexTableName, boolean frequency);

    MimeDecoder createMimeDecoder();
}

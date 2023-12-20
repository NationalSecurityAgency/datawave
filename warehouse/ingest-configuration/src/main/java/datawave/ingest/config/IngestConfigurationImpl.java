package datawave.ingest.config;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;

import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.Type;
import datawave.ingest.data.config.MarkingsHelper;
import datawave.ingest.data.config.MaskedFieldHelper;
import datawave.ingest.metadata.EventMetadata;
import datawave.ingest.metadata.RawRecordMetadata;

public class IngestConfigurationImpl implements IngestConfiguration {

    @Override
    public MarkingsHelper getMarkingsHelper(Configuration config, Type dataType) {
        return new MarkingsHelper.NoOp(config, dataType);
    }

    @Override
    public MaskedFieldHelper createMaskedFieldHelper() {
        return null;
    }

    @Override
    public RawRecordContainer createRawRecordContainer() {
        return new RawRecordContainerImpl();
    }

    @Override
    public MimeDecoder createMimeDecoder() {
        return new MimeDecoderImpl();
    }

    @Override
    public RawRecordMetadata createMetadata(Text shardTableName, Text metadataTableName, Text loadDatesTableName, Text shardIndexTableName,
                    Text shardReverseIndexTableName, boolean frequency) {
        return new EventMetadata(shardTableName, metadataTableName, loadDatesTableName, shardIndexTableName, shardReverseIndexTableName, frequency);
    }
}

package nsa.datawave.ingest.config;

import nsa.datawave.ingest.data.RawRecordContainer;
import nsa.datawave.ingest.data.Type;
import nsa.datawave.ingest.data.config.MarkingsHelper;
import nsa.datawave.ingest.data.config.MaskedFieldHelper;
import nsa.datawave.ingest.metadata.EventMetadata;
import nsa.datawave.ingest.metadata.RawRecordMetadata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;

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

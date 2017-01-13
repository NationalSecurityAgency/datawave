package nsa.datawave.ingest.config;

import nsa.datawave.ingest.data.RawRecordContainer;
import nsa.datawave.ingest.data.Type;
import nsa.datawave.ingest.data.config.MarkingsHelper;
import nsa.datawave.ingest.data.config.MaskedFieldHelper;
import nsa.datawave.ingest.metadata.RawRecordMetadata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;

public interface IngestConfiguration {
    
    MarkingsHelper getMarkingsHelper(Configuration config, Type dataType);
    
    MaskedFieldHelper createMaskedFieldHelper();
    
    RawRecordContainer createRawRecordContainer();
    
    RawRecordMetadata createMetadata(Text shardTableName, Text metadataTableName, Text loadDatesTableName, Text shardIndexTableName,
                    Text shardReverseIndexTableName, boolean frequency);
    
    MimeDecoder createMimeDecoder();
}

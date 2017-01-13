package nsa.datawave.ingest.config;

import java.io.IOException;

import javax.mail.internet.MimeUtility;

import nsa.datawave.ingest.data.RawRecordContainer;
import nsa.datawave.ingest.data.Type;
import nsa.datawave.ingest.data.config.MarkingsHelper;
import nsa.datawave.ingest.data.config.MaskedFieldHelper;
import nsa.datawave.ingest.data.config.NormalizedContentInterface;
import nsa.datawave.ingest.data.config.ingest.IngestHelperInterface;
import nsa.datawave.ingest.mapreduce.job.BulkIngestKey;
import nsa.datawave.ingest.metadata.EventMetadata;
import nsa.datawave.ingest.metadata.RawRecordMetadata;

import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;

import com.google.common.collect.Multimap;

/**
 * An implementation of {@link IngestConfiguration} used only for tests in this package. This is needed since the version in ingest-core-configuration can't be
 * added as a dependency as that would cause a circular dependency chain.
 */
public class TestIngestConfigurationImpl implements IngestConfiguration {
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
        return null;
    }
    
    @Override
    public MimeDecoder createMimeDecoder() {
        return new MimeDecoder() {
            @Override
            public byte[] decode(byte[] b) throws IOException {
                return MimeUtility.decodeText(new String(b, "iso-8859-1")).getBytes();
            }
        };
    }
    
    @Override
    public RawRecordMetadata createMetadata(Text shardTableName, Text metadataTableName, Text loadDatesTableName, Text shardIndexTableName,
                    Text shardReverseIndexTableName, boolean frequency) {
        return new EventMetadata(shardTableName, metadataTableName, loadDatesTableName, shardIndexTableName, shardReverseIndexTableName, frequency);
    }
}

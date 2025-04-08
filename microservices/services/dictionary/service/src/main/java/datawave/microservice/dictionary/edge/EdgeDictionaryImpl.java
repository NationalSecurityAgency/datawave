package datawave.microservice.dictionary.edge;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.SetMultimap;
import com.google.protobuf.InvalidProtocolBufferException;

import datawave.metadata.protobuf.EdgeMetadata;
import datawave.metadata.protobuf.EdgeMetadata.MetadataValue;
import datawave.query.util.MetadataHelper;
import datawave.query.util.MetadataHelperFactory;
import datawave.util.StringUtils;
import datawave.webservice.dictionary.edge.DefaultEdgeDictionary;
import datawave.webservice.dictionary.edge.DefaultMetadata;
import datawave.webservice.dictionary.edge.EventField;

public class EdgeDictionaryImpl implements EdgeDictionary<DefaultEdgeDictionary,DefaultMetadata> {
    private static final Logger log = LoggerFactory.getLogger(EdgeDictionaryImpl.class);
    
    private final MetadataHelperFactory metadataHelperFactory;
    
    public EdgeDictionaryImpl(MetadataHelperFactory metadataHelperFactory) {
        this.metadataHelperFactory = metadataHelperFactory;
    }
    
    @Override
    public DefaultEdgeDictionary getEdgeDictionary(String metadataTableName, AccumuloClient accumuloClient, Set<Authorizations> auths, int numThreads)
                    throws Exception {
        
        MetadataHelper metadataHelper = this.metadataHelperFactory.createMetadataHelper(accumuloClient, metadataTableName, auths);
        
        // Convert them into a response object
        
        // Convert them into the DataDictionary response object
        return transformResults(metadataHelper.getEdges());
    }
    
    // consume the iterator, parse key/value pairs into Metadata stuff
    private DefaultEdgeDictionary transformResults(SetMultimap<Key,Value> edgeMetadataRows) {
        final Text row = new Text(), cf = new Text(), cq = new Text();
        
        Collection<DefaultMetadata> metadata = new LinkedList<>();
        // Each Entry is the entire row
        for (Entry<Key,Value> edgeMetadataRow : edgeMetadataRows.entries()) {
            DefaultMetadata meta = new DefaultMetadata();
            String startDate = null; // Earliest date of collection
            
            // Handle batch scanner bug
            if (edgeMetadataRow.getKey() == null && edgeMetadataRow.getValue() == null)
                return null;
            if (null == edgeMetadataRow.getKey() || null == edgeMetadataRow.getValue()) {
                throw new IllegalArgumentException("Null key or value. Key:" + edgeMetadataRow.getKey() + ", Value: " + edgeMetadataRow.getValue());
            }
            
            Key key = edgeMetadataRow.getKey();
            Value value = edgeMetadataRow.getValue();
            
            // Parse row/cf/cq for
            // TODO create meta key / value helper classes
            key.getRow(row);
            key.getColumnFamily(cf);
            key.getColumnQualifier(cq);
            
            String[] pieces = StringUtils.split(row.toString(), COL_SEPARATOR);
            if (pieces.length != 2) {
                throw new IllegalArgumentException("Invalid Edge Metadata Key:" + edgeMetadataRow.getKey());
            }
            meta.setEdgeType(pieces[0]);
            meta.setEdgeRelationship(pieces[1]);
            meta.setEdgeAttribute1Source(cq.toString());
            
            // Parse the Value
            MetadataValue metadataVal;
            try {
                metadataVal = EdgeMetadata.MetadataValue.parseFrom(value.get());
            } catch (InvalidProtocolBufferException e) {
                log.error("Found invalid Edge Metadata Value bytes.");
                continue;
            }
            List<EventField> eFields = new LinkedList<>();
            for (MetadataValue.Metadata metaval : metadataVal.getMetadataList()) {
                EventField eField = new EventField();
                eField.setSourceField(metaval.getSource());
                eField.setSinkField(metaval.getSink());
                if (metaval.hasEnrichment()) {
                    eField.setEnrichmentField(metaval.getEnrichment());
                    eField.setEnrichmentIndex(metaval.getEnrichmentIndex());
                }
                
                if (metaval.hasJexlPrecondition()) {
                    eField.setJexlPrecondition(metaval.getJexlPrecondition());
                }
                eFields.add(eField);
                
                if (metaval.hasDate()) {
                    if (startDate == null) {
                        startDate = metaval.getDate();
                    } else if (startDate.compareTo(metaval.getDate()) > 0) {
                        startDate = metaval.getDate();
                    }
                }
            }
            
            meta.setStartDate(startDate);
            meta.setEventFields(eFields);
            metadata.add(meta);
        }
        return new DefaultEdgeDictionary(metadata);
    }
}

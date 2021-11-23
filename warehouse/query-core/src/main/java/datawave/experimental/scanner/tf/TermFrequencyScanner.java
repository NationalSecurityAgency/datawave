package datawave.experimental.scanner.tf;

import java.util.Map;
import java.util.Set;

import org.apache.commons.jexl2.parser.ASTJexlScript;

import datawave.query.attributes.Document;

public interface TermFrequencyScanner {

    Map<String,Object> fetchOffsets(ASTJexlScript script, Document d, String shard, String uid);

    void setTermFrequencyFields(Set<String> termFrequencyFields);
}

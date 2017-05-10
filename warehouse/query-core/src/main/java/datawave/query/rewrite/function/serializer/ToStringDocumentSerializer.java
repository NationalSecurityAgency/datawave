package datawave.query.rewrite.function.serializer;

import datawave.query.rewrite.attributes.Document;

public class ToStringDocumentSerializer extends DocumentSerializer {
    
    public ToStringDocumentSerializer(boolean reducedResponse) {
        super(reducedResponse, false);
    }
    
    @Override
    public byte[] serialize(Document doc) {
        return doc.toString().getBytes();
    }
    
}

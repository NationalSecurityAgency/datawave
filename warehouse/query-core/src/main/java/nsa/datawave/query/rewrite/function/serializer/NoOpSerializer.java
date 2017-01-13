package nsa.datawave.query.rewrite.function.serializer;

import nsa.datawave.query.rewrite.attributes.Document;

public class NoOpSerializer extends DocumentSerializer {
    private static final byte[] EMPTY_BYTES = new byte[0];
    
    public NoOpSerializer(boolean reducedResponse) {
        super(reducedResponse, false);
    }
    
    @Override
    public byte[] serialize(Document doc) {
        return EMPTY_BYTES;
    }
    
}

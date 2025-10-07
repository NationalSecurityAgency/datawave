package datawave.query.function.serializer;

import datawave.query.attributes.Document;

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

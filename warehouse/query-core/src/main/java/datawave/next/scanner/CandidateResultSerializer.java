package datawave.next.scanner;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class CandidateResultSerializer {

    private final Kryo kryo = new Kryo();
    private final ByteArrayOutputStream baos = new ByteArrayOutputStream();

    public byte[] serialize(CandidateResult result) throws IOException {
        baos.reset();
        try (var output = new Output(baos)) {
            result.write(kryo, output);
        }
        return baos.toByteArray();
    }

    public CandidateResult deserialize(byte[] bytes) throws IOException {
        CandidateResult result = new CandidateResult();
        try (var input = new Input(bytes)) {
            result.read(kryo, input);
        }
        return result;
    }
}

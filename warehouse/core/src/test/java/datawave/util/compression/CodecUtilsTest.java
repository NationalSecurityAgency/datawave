package datawave.util.compression;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class CodecUtilsTest {

    private static final String DATA = "field1:LcNoDiacriticsType;field2:NumberType — café";

    @ParameterizedTest
    @EnumSource(CodecAlgorithm.class)
    void compressDispatchesToAlgorithmCodec(CodecAlgorithm algorithm) {
        String compressed = CodecUtils.compress(DATA, StandardCharsets.UTF_8, algorithm);
        // the codec output is deterministic, so CodecUtils must produce exactly what the codec does
        assertEquals(algorithm.getCodec().compress(DATA, StandardCharsets.UTF_8), compressed);
        assertEquals(DATA, algorithm.getCodec().decompress(compressed, StandardCharsets.UTF_8));
    }

    @ParameterizedTest
    @EnumSource(CodecAlgorithm.class)
    void decompressDispatchesToAlgorithmCodec(CodecAlgorithm algorithm) {
        String compressed = algorithm.getCodec().compress(DATA, StandardCharsets.UTF_8);
        assertEquals(DATA, CodecUtils.decompress(compressed, StandardCharsets.UTF_8, algorithm));
    }

    @Test
    void constructorThrows() throws Exception {
        Constructor<CodecUtils> constructor = CodecUtils.class.getDeclaredConstructor();
        constructor.setAccessible(true);
        InvocationTargetException e = assertThrows(InvocationTargetException.class, constructor::newInstance);
        assertInstanceOf(UnsupportedOperationException.class, e.getCause());
    }
}

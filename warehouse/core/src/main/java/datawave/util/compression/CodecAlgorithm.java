package datawave.util.compression;

/**
 * The supported compression algorithms, each holding the singleton {@link Codec} that implements it.
 */
public enum CodecAlgorithm {

    GZIP(GZIPCodec.instance()),
    BZIP2(BZIP2Codec.instance()),
    // enum constants cannot start with a digit, hence SEVEN_ZIP rather than 7ZIP
    SEVEN_ZIP(SevenZIPCodec.instance());

    private final Codec codec;

    CodecAlgorithm(Codec codec) {
        this.codec = codec;
    }

    public Codec getCodec() {
        return codec;
    }
}

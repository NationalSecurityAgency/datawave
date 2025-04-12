package datawave.annotation.util.v1;

import com.google.protobuf.util.JsonFormat;

import datawave.annotation.protobuf.v1.Segment;

public class SegmentJsonConverter {
    private static final JsonFormat.Printer PRINTER = JsonFormat.printer().preservingProtoFieldNames();
    private static final JsonFormat.Parser PARSER = JsonFormat.parser().ignoringUnknownFields();

    private SegmentJsonConverter() {}

    public static String getBoundaryCaseString(Segment.BoundaryCase boundaryCase) {
        switch (boundaryCase) {
            case ALL:
                return "ENTIRE";
            case POINTLIST:
                return "POINTLIST";
            case TIME:
                return "TIME";
            case CHARACTERS:
                return "CHARACTERS";
            case BOUNDARY_NOT_SET:
            default:
                return "";
        }
    }

    public static String toJsonWithDiscriminator(Segment s) throws Exception {
        String type = getBoundaryCaseString(s.getBoundaryCase());
        Segment withType = s.toBuilder().setBoundaryType(type).build();
        return PRINTER.print(withType);
    }

    public static Segment fromJson(String json) throws Exception {
        Segment.Builder b = Segment.newBuilder();
        PARSER.merge(json, b);
        return b.build();
    }
}

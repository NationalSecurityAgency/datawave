package datawave.query.util;

import static datawave.util.TableName.METADATA;
import static datawave.util.TableName.SHARD;
import static datawave.util.TableName.SHARD_INDEX;
import static datawave.util.TableName.SHARD_RINDEX;

import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.LongCombiner;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;

import datawave.data.ColumnFamilyConstants;
import datawave.data.hash.UID;
import datawave.data.type.LcNoDiacriticsListType;
import datawave.data.type.LcNoDiacriticsType;
import datawave.data.type.ListType;
import datawave.data.type.NumberType;
import datawave.ingest.protobuf.Uid;
import datawave.util.TableName;

/**
 * Data for complex testing of ingest types, normalizers, query models, visibilities and more.
 * <p>
 * Also tests indexed fields, event-only fields, index-only fields, and non-event fields.
 * <p>
 * Fields
 * <ul>
 * <li>DESCRIPTION: indexed, reverse indexed, index-only, tokenized</li>
 * <li>EDGES: indexed, Number type</li>
 * <li>PROPERTY: event only, tokenized</li>
 * <li>SHAPE: indexed, reverse indexed</li>
 * <li>TYPE: indexed, reverse indexed</li>
 * <li>UUID: event only</li>
 * <li>ONLY_TRI: indexed</li>
 * <li>ONLY_QUAD: indexed</li>
 * <li>ONLY_PENTA: indexed</li>
 * <li>ONLY_HEX: indexed</li>
 * <li>ONLY_OCT: indexed</li>
 * </ul>
 * Shapes
 * <ul>
 * <li>triangle - acute</li>
 * <li>triangle - equilateral</li>
 * <li>triangle - isosceles</li>
 * <li>quadrilateral - parallelogram - square</li>
 * <li>quadrilateral - parallelogram - rectangle</li>
 * <li>quadrilateral - parallelogram - rhombus</li>
 * <li>quadrilateral - parallelogram - rhomboid</li>
 * <li>quadrilateral - trapezoid</li>
 * <li>quadrilateral - kite</li>
 * <li>pentagon</li>
 * <li>hexagon</li>
 * <li>octagon</li>
 * </ul>
 */
public class ShapesIngest {

    public enum RangeType {
        SHARD, DOCUMENT
    }

    private static final String shard = "20240202_0";

    private static final String triangle = "triangle";
    private static final String quadrilateral = "quadrilateral";
    private static final String pentagon = "pentagon";
    private static final String hexagon = "hexagon";
    private static final String octagon = "octagon";

    // triangle
    public static final String acuteUid = UID.builder().newId("acute".getBytes(), (Date) null).toString();
    public static final String equilateralUid = UID.builder().newId("equilateral".getBytes(), (Date) null).toString();
    public static final String isoscelesUid = UID.builder().newId("isosceles".getBytes(), (Date) null).toString();

    // quadrilateral - parallelogram
    public static final String squareUid = UID.builder().newId("square".getBytes(), (Date) null).toString();
    public static final String rectangleUid = UID.builder().newId("rectangle".getBytes(), (Date) null).toString();
    public static final String rhombusUid = UID.builder().newId("rhombus".getBytes(), (Date) null).toString();
    public static final String rhomboidUid = UID.builder().newId("rhomboid".getBytes(), (Date) null).toString();

    // quadrilaterals
    public static final String trapezoidUid = UID.builder().newId("trapezoid".getBytes(), (Date) null).toString();
    public static final String kiteUid = UID.builder().newId("kite".getBytes(), (Date) null).toString();
    // additional polygon
    public static final String pentagonUid = UID.builder().newId("pentagon".getBytes(), (Date) null).toString();
    public static final String hexagonUid = UID.builder().newId("hexagon".getBytes(), (Date) null).toString();
    public static final String octagonUid = UID.builder().newId("octagon".getBytes(), (Date) null).toString();

    private static final ColumnVisibility cv = new ColumnVisibility("ALL");
    private static final Long ts = 1707045600000L;
    private static final Value value = new Value();

    private static final NumberType number = new NumberType();
    private static final LcNoDiacriticsListType list = new LcNoDiacriticsListType();

    private static final LongCombiner.VarLenEncoder encoder = new LongCombiner.VarLenEncoder();

    protected static String normalizerForField(String field) {
        switch (field) {
            case "SHAPE":
            case "TYPE":
            case "ONLY_TRI":
            case "ONLY_QUAD":
            case "ONLY_PENTA":
            case "ONLY_HEX":
            case "ONLY_OCT":
            case "UUID":
                return LcNoDiacriticsType.class.getName();
            case "EDGES":
                return NumberType.class.getName();
            case "DESCRIPTION":
            case "PROPERTIES":
                return ListType.class.getName();
            default:
                throw new IllegalArgumentException("No normalizer configured for field: " + field);
        }
    }

    public static void writeData(AccumuloClient client, RangeType type) throws Exception {

        TableOperations tops = client.tableOperations();
        tops.create(SHARD);
        tops.create(SHARD_INDEX);
        tops.create(SHARD_RINDEX);
        tops.create(METADATA);

        BatchWriterConfig bwConfig = new BatchWriterConfig().setMaxMemory(1000L).setMaxLatency(1, TimeUnit.SECONDS).setMaxWriteThreads(1);
        Mutation m;

        // shard data
        try (BatchWriter bw = client.createBatchWriter(SHARD, bwConfig)) {
            m = new Mutation(shard);

            // acute triangle
            String acuteCF = triangle + '\u0000' + acuteUid;
            m.put(acuteCF, "EDGES\u00003", cv, ts, value);
            m.put(acuteCF, "SHAPE\u0000triangle", cv, ts, value);
            m.put(acuteCF, "TYPE\u0000acute", cv, ts, value);
            m.put(acuteCF, "ONLY_TRI\u0000tri", cv, ts, value);
            m.put(acuteCF, "UUID\u0000" + acuteUid, cv, ts, value);

            // equilateral triangle
            String equilateralCF = triangle + '\u0000' + equilateralUid;
            m.put(equilateralCF, "EDGES\u00003", cv, ts, value);
            m.put(equilateralCF, "SHAPE\u0000triangle", cv, ts, value);
            m.put(equilateralCF, "TYPE\u0000equilateral", cv, ts, value);
            m.put(equilateralCF, "ONLY_TRI\u0000tri", cv, ts, value);
            m.put(equilateralCF, "UUID\u0000" + equilateralUid, cv, ts, value);

            // isosceles triangle
            String isoscelesCF = triangle + '\u0000' + isoscelesUid;
            m.put(isoscelesCF, "EDGES\u00003", cv, ts, value);
            m.put(isoscelesCF, "SHAPE\u0000triangle", cv, ts, value);
            m.put(isoscelesCF, "TYPE\u0000isosceles", cv, ts, value);
            m.put(isoscelesCF, "ONLY_TRI\u0000tri", cv, ts, value);
            m.put(isoscelesCF, "UUID\u0000" + isoscelesUid, cv, ts, value);

            // quadrilateral - parallelogram - square
            String squareCF = quadrilateral + '\u0000' + squareUid;
            m.put(squareCF, "EDGES\u00004", cv, ts, value);
            m.put(squareCF, "SHAPE\u0000quadrilateral", cv, ts, value);
            m.put(squareCF, "TYPE\u0000square", cv, ts, value);
            m.put(squareCF, "ONLY_QUAD\u0000quad", cv, ts, value);
            m.put(squareCF, "UUID\u0000" + squareUid, cv, ts, value);

            // quadrilateral - parallelogram - rectangle
            String rectangleCF = quadrilateral + '\u0000' + rectangleUid;
            m.put(rectangleCF, "EDGES\u00004", cv, ts, value);
            m.put(rectangleCF, "SHAPE\u0000quadrilateral", cv, ts, value);
            m.put(rectangleCF, "TYPE\u0000rectangle", cv, ts, value);
            m.put(rectangleCF, "ONLY_QUAD\u0000quad", cv, ts, value);
            m.put(rectangleCF, "UUID\u0000" + rectangleUid, cv, ts, value);

            // quadrilateral - parallelogram - rhombus
            String rhombusCF = quadrilateral + '\u0000' + rhombusUid;
            m.put(rhombusCF, "EDGES\u00004", cv, ts, value);
            m.put(rhombusCF, "SHAPE\u0000quadrilateral", cv, ts, value);
            m.put(rhombusCF, "TYPE\u0000rhombus", cv, ts, value);
            m.put(rhombusCF, "ONLY_QUAD\u0000quad", cv, ts, value);
            m.put(rhombusCF, "UUID\u0000" + rhombusUid, cv, ts, value);

            // quadrilateral - parallelogram - rhomboid
            String rhomboidCF = quadrilateral + '\u0000' + rhomboidUid;
            m.put(rhomboidCF, "EDGES\u00004", cv, ts, value);
            m.put(rhomboidCF, "SHAPE\u0000quadrilateral", cv, ts, value);
            m.put(rhomboidCF, "TYPE\u0000rhomboid", cv, ts, value);
            m.put(rhomboidCF, "ONLY_QUAD\u0000quad", cv, ts, value);
            m.put(rhomboidCF, "UUID\u0000" + rhomboidUid, cv, ts, value);

            // quadrilateral - trapezoid
            String trapezoidCF = quadrilateral + '\u0000' + trapezoidUid;
            m.put(trapezoidCF, "EDGES\u00004", cv, ts, value);
            m.put(trapezoidCF, "SHAPE\u0000quadrilateral", cv, ts, value);
            m.put(trapezoidCF, "TYPE\u0000trapezoid", cv, ts, value);
            m.put(trapezoidCF, "ONLY_QUAD\u0000quad", cv, ts, value);
            m.put(trapezoidCF, "UUID\u0000" + trapezoidUid, cv, ts, value);

            // quadrilateral - kite
            String kiteCF = quadrilateral + '\u0000' + kiteUid;
            m.put(kiteCF, "EDGES\u00004", cv, ts, value);
            m.put(kiteCF, "SHAPE\u0000quadrilateral", cv, ts, value);
            m.put(kiteCF, "TYPE\u0000kite", cv, ts, value);
            m.put(kiteCF, "ONLY_QUAD\u0000quad", cv, ts, value);
            m.put(kiteCF, "UUID\u0000" + kiteUid, cv, ts, value);

            // pentagon
            String pentagonCF = pentagon + '\u0000' + pentagonUid;
            m.put(pentagonCF, "EDGES\u00005", cv, ts, value);
            m.put(pentagonCF, "SHAPE\u0000pentagon", cv, ts, value);
            m.put(pentagonCF, "TYPE\u0000regular", cv, ts, value);
            m.put(pentagonCF, "ONLY_PENTA\u0000penta", cv, ts, value);
            m.put(pentagonCF, "UUID\u0000" + pentagonUid, cv, ts, value);

            // hexagon
            String hexagonCF = hexagon + '\u0000' + hexagonUid;
            m.put(hexagonCF, "EDGES\u00006", cv, ts, value);
            m.put(hexagonCF, "SHAPE\u0000hexagon", cv, ts, value);
            m.put(hexagonCF, "TYPE\u0000regular", cv, ts, value);
            m.put(hexagonCF, "ONLY_HEX\u0000hexa", cv, ts, value);
            m.put(hexagonCF, "UUID\u0000" + hexagonUid, cv, ts, value);

            // octagon
            String octagonCF = octagon + '\u0000' + octagonUid;
            m.put(octagonCF, "EDGES\u00008", cv, ts, value);
            m.put(octagonCF, "SHAPE\u0000octagon", cv, ts, value);
            m.put(octagonCF, "TYPE\u0000regular", cv, ts, value);
            m.put(octagonCF, "ONLY_OCT\u0000octa", cv, ts, value);
            m.put(octagonCF, "UUID\u0000" + octagonUid, cv, ts, value);

            bw.addMutation(m);
        }

        // field index data
        try (BatchWriter bw = client.createBatchWriter(SHARD, bwConfig)) {
            // SHAPE
            m = new Mutation(shard);
            m.put("fi\0SHAPE", "triangle\0" + triangle + "\0" + acuteUid, cv, ts, value);
            m.put("fi\0SHAPE", "triangle\0" + triangle + "\0" + equilateralUid, cv, ts, value);
            m.put("fi\0SHAPE", "triangle\0" + triangle + "\0" + isoscelesUid, cv, ts, value);
            m.put("fi\0SHAPE", "quadrilateral\0" + quadrilateral + "\0" + squareUid, cv, ts, value);
            m.put("fi\0SHAPE", "quadrilateral\0" + quadrilateral + "\0" + rectangleUid, cv, ts, value);
            m.put("fi\0SHAPE", "quadrilateral\0" + quadrilateral + "\0" + rhomboidUid, cv, ts, value);
            m.put("fi\0SHAPE", "quadrilateral\0" + quadrilateral + "\0" + rhombusUid, cv, ts, value);
            m.put("fi\0SHAPE", "quadrilateral\0" + quadrilateral + "\0" + trapezoidUid, cv, ts, value);
            m.put("fi\0SHAPE", "quadrilateral\0" + quadrilateral + "\0" + kiteUid, cv, ts, value);
            m.put("fi\0SHAPE", "pentagon\0" + pentagon + "\0" + pentagonUid, cv, ts, value);
            m.put("fi\0SHAPE", "hexagon\0" + hexagon + "\0" + hexagonUid, cv, ts, value);
            m.put("fi\0SHAPE", "octagon\0" + octagon + "\0" + octagonUid, cv, ts, value);

            // TYPE
            m.put("fi\0TYPE", "acute\0" + triangle + "\0" + acuteUid, cv, ts, value);
            m.put("fi\0TYPE", "equilateral\0" + triangle + "\0" + equilateralUid, cv, ts, value);
            m.put("fi\0TYPE", "isosceles\0" + triangle + "\0" + isoscelesUid, cv, ts, value);
            m.put("fi\0TYPE", "square\0" + quadrilateral + "\0" + squareUid, cv, ts, value);
            m.put("fi\0TYPE", "rectangle\0" + quadrilateral + "\0" + rectangleUid, cv, ts, value);
            m.put("fi\0TYPE", "rhomboid\0" + quadrilateral + "\0" + rhomboidUid, cv, ts, value);
            m.put("fi\0TYPE", "rhombus\0" + quadrilateral + "\0" + rhombusUid, cv, ts, value);
            m.put("fi\0TYPE", "trapezoid\0" + quadrilateral + "\0" + trapezoidUid, cv, ts, value);
            m.put("fi\0TYPE", "kite\0" + quadrilateral + "\0" + kiteUid, cv, ts, value);
            m.put("fi\0TYPE", "regular\0" + pentagon + "\0" + pentagonUid, cv, ts, value);
            m.put("fi\0TYPE", "regular\0" + hexagon + "\0" + hexagonUid, cv, ts, value);
            m.put("fi\0TYPE", "regular\0" + octagon + "\0" + octagonUid, cv, ts, value);

            // EDGES
            m.put("fi\0EDGES", number.normalize("3") + "\0" + triangle + "\0" + acuteUid, cv, ts, value);
            m.put("fi\0EDGES", number.normalize("3") + "\0" + triangle + "\0" + equilateralUid, cv, ts, value);
            m.put("fi\0EDGES", number.normalize("3") + "\0" + triangle + "\0" + isoscelesUid, cv, ts, value);
            m.put("fi\0EDGES", number.normalize("4") + "\0" + quadrilateral + "\0" + squareUid, cv, ts, value);
            m.put("fi\0EDGES", number.normalize("4") + "\0" + quadrilateral + "\0" + rectangleUid, cv, ts, value);
            m.put("fi\0EDGES", number.normalize("4") + "\0" + quadrilateral + "\0" + rhomboidUid, cv, ts, value);
            m.put("fi\0EDGES", number.normalize("4") + "\0" + quadrilateral + "\0" + rhombusUid, cv, ts, value);
            m.put("fi\0EDGES", number.normalize("4") + "\0" + quadrilateral + "\0" + trapezoidUid, cv, ts, value);
            m.put("fi\0EDGES", number.normalize("4") + "\0" + quadrilateral + "\0" + kiteUid, cv, ts, value);
            m.put("fi\0EDGES", number.normalize("5") + "\0" + pentagon + "\0" + pentagonUid, cv, ts, value);
            m.put("fi\0EDGES", number.normalize("6") + "\0" + hexagon + "\0" + hexagonUid, cv, ts, value);
            m.put("fi\0EDGES", number.normalize("7") + "\0" + octagon + "\0" + octagonUid, cv, ts, value);

            // ONLY_*
            m.put("fi\0ONLY_TRI", "tri\0" + triangle + "\0" + acuteUid, cv, ts, value);
            m.put("fi\0ONLY_TRI", "tri\0" + triangle + "\0" + equilateralUid, cv, ts, value);
            m.put("fi\0ONLY_TRI", "tri\0" + triangle + "\0" + isoscelesUid, cv, ts, value);
            m.put("fi\0ONLY_QUAD", "quad\0" + quadrilateral + "\0" + squareUid, cv, ts, value);
            m.put("fi\0ONLY_QUAD", "quad\0" + quadrilateral + "\0" + rectangleUid, cv, ts, value);
            m.put("fi\0ONLY_QUAD", "quad\0" + quadrilateral + "\0" + rhomboidUid, cv, ts, value);
            m.put("fi\0ONLY_QUAD", "quad\0" + quadrilateral + "\0" + rhombusUid, cv, ts, value);
            m.put("fi\0ONLY_QUAD", "quad\0" + quadrilateral + "\0" + trapezoidUid, cv, ts, value);
            m.put("fi\0ONLY_QUAD", "quad\0" + quadrilateral + "\0" + kiteUid, cv, ts, value);
            m.put("fi\0ONLY_PENTA", "penta\0" + pentagon + "\0" + pentagonUid, cv, ts, value);
            m.put("fi\0ONLY_HEX", "hexa\0" + hexagon + "\0" + hexagonUid, cv, ts, value);
            m.put("fi\0ONLY_OCT", "octa\0" + octagon + "\0" + octagonUid, cv, ts, value);

            bw.addMutation(m);
        }

        // shard index data
        try (BatchWriter bw = client.createBatchWriter(TableName.SHARD_INDEX, bwConfig)) {

            // SHAPE
            m = new Mutation("triangle");
            m.put("SHAPE", shard + '\u0000' + triangle, cv, ts, getValue(type, acuteUid));
            m.put("SHAPE", shard + '\u0000' + triangle, cv, ts, getValue(type, equilateralUid));
            m.put("SHAPE", shard + '\u0000' + triangle, cv, ts, getValue(type, isoscelesUid));
            bw.addMutation(m);
            m = new Mutation("quadrilateral");
            m.put("SHAPE", shard + '\u0000' + quadrilateral, cv, ts, getValue(type, squareUid));
            m.put("SHAPE", shard + '\u0000' + quadrilateral, cv, ts, getValue(type, rectangleUid));
            m.put("SHAPE", shard + '\u0000' + quadrilateral, cv, ts, getValue(type, rhomboidUid));
            m.put("SHAPE", shard + '\u0000' + quadrilateral, cv, ts, getValue(type, rhombusUid));
            bw.addMutation(m);
            m = new Mutation("pentagon");
            m.put("SHAPE", shard + '\u0000' + pentagon, cv, ts, getValue(type, pentagonUid));
            bw.addMutation(m);
            m = new Mutation("hexagon");
            m.put("SHAPE", shard + '\u0000' + hexagon, cv, ts, getValue(type, hexagonUid));
            bw.addMutation(m);
            m = new Mutation("octagon");
            m.put("SHAPE", shard + '\u0000' + octagon, cv, ts, getValue(type, octagonUid));
            bw.addMutation(m);

            // TYPE
            m = new Mutation("acute");
            m.put("TYPE", shard + '\u0000' + triangle, cv, ts, getValue(type, acuteUid));
            bw.addMutation(m);
            m = new Mutation("equilateral");
            m.put("TYPE", shard + '\u0000' + triangle, cv, ts, getValue(type, equilateralUid));
            bw.addMutation(m);
            m = new Mutation("isosceles");
            m.put("TYPE", shard + '\u0000' + triangle, cv, ts, getValue(type, isoscelesUid));
            bw.addMutation(m);
            m = new Mutation("square");
            m.put("TYPE", shard + '\u0000' + quadrilateral, cv, ts, getValue(type, squareUid));
            bw.addMutation(m);
            m = new Mutation("rectangle");
            m.put("TYPE", shard + '\u0000' + quadrilateral, cv, ts, getValue(type, rectangleUid));
            bw.addMutation(m);
            m = new Mutation("rhombus");
            m.put("TYPE", shard + '\u0000' + quadrilateral, cv, ts, getValue(type, rhombusUid));
            bw.addMutation(m);
            m = new Mutation("rhomboid");
            m.put("TYPE", shard + '\u0000' + quadrilateral, cv, ts, getValue(type, rhomboidUid));
            bw.addMutation(m);
            m = new Mutation("pentagon");
            m.put("TYPE", shard + '\u0000' + pentagon, cv, ts, getValue(type, pentagonUid));
            bw.addMutation(m);
            m = new Mutation("hexagon");
            m.put("TYPE", shard + '\u0000' + hexagon, cv, ts, getValue(type, hexagonUid));
            bw.addMutation(m);
            m = new Mutation("octagon");
            m.put("TYPE", shard + '\u0000' + octagon, cv, ts, getValue(type, octagonUid));
            bw.addMutation(m);

            // EDGES
            m = new Mutation(number.normalize("3"));
            m.put("EDGES", shard + '\u0000' + triangle, cv, ts, getValue(type, acuteUid));
            m.put("EDGES", shard + '\u0000' + triangle, cv, ts, getValue(type, equilateralUid));
            m.put("EDGES", shard + '\u0000' + triangle, cv, ts, getValue(type, isoscelesUid));
            bw.addMutation(m);

            m = new Mutation(number.normalize("4"));
            m.put("EDGES", shard + '\u0000' + triangle, cv, ts, getValue(type, isoscelesUid));
            bw.addMutation(m);

            m = new Mutation(number.normalize("5"));
            m.put("EDGES", shard + '\u0000' + triangle, cv, ts, getValue(type, isoscelesUid));
            bw.addMutation(m);

            m = new Mutation(number.normalize("6"));
            m.put("EDGES", shard + '\u0000' + triangle, cv, ts, getValue(type, isoscelesUid));
            bw.addMutation(m);

            m = new Mutation(number.normalize("8"));
            m.put("EDGES", shard + '\u0000' + triangle, cv, ts, getValue(type, isoscelesUid));
            bw.addMutation(m);

            // ONLY_*
            m = new Mutation("tri");
            m.put("ONLY_TRI", shard + '\u0000' + triangle, cv, ts, getValue(type, isoscelesUid));
            m.put("ONLY_TRI", shard + '\u0000' + triangle, cv, ts, getValue(type, equilateralUid));
            m.put("ONLY_TRI", shard + '\u0000' + triangle, cv, ts, getValue(type, isoscelesUid));
            bw.addMutation(m);
            m = new Mutation("quad");
            m.put("ONLY_QUAD", shard + '\u0000' + quadrilateral, cv, ts, getValue(type, squareUid));
            m.put("ONLY_QUAD", shard + '\u0000' + quadrilateral, cv, ts, getValue(type, rectangleUid));
            m.put("ONLY_QUAD", shard + '\u0000' + quadrilateral, cv, ts, getValue(type, rhomboidUid));
            m.put("ONLY_QUAD", shard + '\u0000' + quadrilateral, cv, ts, getValue(type, rhombusUid));
            m.put("ONLY_QUAD", shard + '\u0000' + quadrilateral, cv, ts, getValue(type, trapezoidUid));
            m.put("ONLY_QUAD", shard + '\u0000' + quadrilateral, cv, ts, getValue(type, kiteUid));
            bw.addMutation(m);
            m = new Mutation("penta");
            m.put("ONLY_PENTA", shard + '\u0000' + pentagon, cv, ts, getValue(type, pentagonUid));
            bw.addMutation(m);
            m = new Mutation("hexa");
            m.put("ONLY_HEX", shard + '\u0000' + hexagon, cv, ts, getValue(type, hexagonUid));
            bw.addMutation(m);
            m = new Mutation("octa");
            m.put("ONLY_OCT", shard + '\u0000' + octagon, cv, ts, getValue(type, octagonUid));
            bw.addMutation(m);
        }

        // shard reverse index data
        try (BatchWriter bw = client.createBatchWriter(TableName.SHARD_RINDEX, bwConfig)) {
            // SHAPE
            m = new Mutation(new StringBuilder("triangle").reverse());
            m.put("SHAPE", shard + '\u0000' + triangle, cv, ts, getValue(type, acuteUid));
            m.put("SHAPE", shard + '\u0000' + triangle, cv, ts, getValue(type, equilateralUid));
            m.put("SHAPE", shard + '\u0000' + triangle, cv, ts, getValue(type, isoscelesUid));
            bw.addMutation(m);

            m = new Mutation(new StringBuilder("quadrilateral").reverse());
            m.put("SHAPE", shard + '\u0000' + quadrilateral, cv, ts, getValue(type, squareUid));
            m.put("SHAPE", shard + '\u0000' + quadrilateral, cv, ts, getValue(type, rectangleUid));
            m.put("SHAPE", shard + '\u0000' + quadrilateral, cv, ts, getValue(type, rhomboidUid));
            m.put("SHAPE", shard + '\u0000' + quadrilateral, cv, ts, getValue(type, rhombusUid));
            m.put("SHAPE", shard + '\u0000' + quadrilateral, cv, ts, getValue(type, trapezoidUid));
            m.put("SHAPE", shard + '\u0000' + quadrilateral, cv, ts, getValue(type, kiteUid));
            bw.addMutation(m);

            m = new Mutation(new StringBuilder("pentagon").reverse());
            m.put("SHAPE", shard + '\u0000' + pentagon, cv, ts, getValue(type, pentagonUid));
            bw.addMutation(m);

            m = new Mutation(new StringBuilder("hexagon").reverse());
            m.put("SHAPE", shard + '\u0000' + hexagon, cv, ts, getValue(type, hexagonUid));
            bw.addMutation(m);

            m = new Mutation(new StringBuilder("octagon").reverse());
            m.put("SHAPE", shard + '\u0000' + octagon, cv, ts, getValue(type, octagonUid));
            bw.addMutation(m);

            // TYPE
            m = new Mutation(new StringBuilder("acute").reverse());
            m.put("TYPE", shard + '\u0000' + triangle, cv, ts, getValue(type, acuteUid));
            bw.addMutation(m);
            m = new Mutation(new StringBuilder("equilateral").reverse());
            m.put("TYPE", shard + '\u0000' + triangle, cv, ts, getValue(type, equilateralUid));
            bw.addMutation(m);
            m = new Mutation(new StringBuilder("isosceles").reverse());
            m.put("TYPE", shard + '\u0000' + triangle, cv, ts, getValue(type, isoscelesUid));
            bw.addMutation(m);
            m = new Mutation(new StringBuilder("square").reverse());
            m.put("TYPE", shard + '\u0000' + triangle, cv, ts, getValue(type, squareUid));
            bw.addMutation(m);
            m = new Mutation(new StringBuilder("rectangle").reverse());
            m.put("TYPE", shard + '\u0000' + triangle, cv, ts, getValue(type, rectangleUid));
            bw.addMutation(m);
            m = new Mutation(new StringBuilder("rhombus").reverse());
            m.put("TYPE", shard + '\u0000' + triangle, cv, ts, getValue(type, rhombusUid));
            bw.addMutation(m);
            m = new Mutation(new StringBuilder("rhomboid").reverse());
            m.put("TYPE", shard + '\u0000' + triangle, cv, ts, getValue(type, rhomboidUid));
            bw.addMutation(m);
            m = new Mutation(new StringBuilder("pentagon").reverse());
            m.put("TYPE", shard + '\u0000' + triangle, cv, ts, getValue(type, pentagonUid));
            bw.addMutation(m);
            m = new Mutation(new StringBuilder("hexagon").reverse());
            m.put("TYPE", shard + '\u0000' + triangle, cv, ts, getValue(type, hexagonUid));
            bw.addMutation(m);
            m = new Mutation(new StringBuilder("octagon").reverse());
            m.put("TYPE", shard + '\u0000' + triangle, cv, ts, getValue(type, octagonUid));
            bw.addMutation(m);
        }

        // token data written differently
        tokenize(client, bwConfig, "PROPERTIES", "convex,cyclic", type, triangle, acuteUid);
        tokenize(client, bwConfig, "PROPERTIES", "convex,cyclic,equilateral", type, triangle, equilateralUid);
        tokenize(client, bwConfig, "PROPERTIES", "convex,cyclic", type, triangle, isoscelesUid);

        tokenize(client, bwConfig, "PROPERTIES", "convex,cyclic,equilateral,isogonal,isotoxal", type, quadrilateral, squareUid);
        tokenize(client, bwConfig, "PROPERTIES", "convex,isogonal,cyclic", type, quadrilateral, rectangleUid);
        tokenize(client, bwConfig, "PROPERTIES", "convex,isotoxal", type, quadrilateral, rhombusUid);
        tokenize(client, bwConfig, "PROPERTIES", "convex", type, quadrilateral, rhomboidUid);
        tokenize(client, bwConfig, "PROPERTIES", "convex", type, quadrilateral, trapezoidUid);
        tokenize(client, bwConfig, "PROPERTIES", "convex", type, quadrilateral, kiteUid);

        tokenize(client, bwConfig, "PROPERTIES", "convex,cyclic,equilateral,isogonal,isotoxal", type, pentagon, pentagonUid);
        tokenize(client, bwConfig, "PROPERTIES", "convex,cyclic,equilateral,isogonal,isotoxal", type, hexagon, hexagonUid);
        tokenize(client, bwConfig, "PROPERTIES", "convex,cyclic,equilateral,isogonal,isotoxal", type, octagon, octagonUid);

        // metadata table
        try (BatchWriter bw = client.createBatchWriter(TableName.METADATA, bwConfig)) {

            // SHAPE has events (e), frequency (f), is indexed (i), and is reverse indexed (ri)
            m = new Mutation("SHAPE");
            m.put(ColumnFamilyConstants.COLF_E, new Text(triangle), value);
            m.put(ColumnFamilyConstants.COLF_E, new Text(quadrilateral), value);
            m.put(ColumnFamilyConstants.COLF_E, new Text(pentagon), value);
            m.put(ColumnFamilyConstants.COLF_E, new Text(hexagon), value);
            m.put(ColumnFamilyConstants.COLF_E, new Text(octagon), value);

            m.put(ColumnFamilyConstants.COLF_F, new Text(triangle + '\u0000' + shard), createValue(12L));
            m.put(ColumnFamilyConstants.COLF_F, new Text(quadrilateral + '\u0000' + shard), createValue(13L));
            m.put(ColumnFamilyConstants.COLF_F, new Text(pentagon + '\u0000' + shard), createValue(11L));
            m.put(ColumnFamilyConstants.COLF_F, new Text(hexagon + '\u0000' + shard), createValue(10L));
            m.put(ColumnFamilyConstants.COLF_F, new Text(octagon + '\u0000' + shard), createValue(14L));

            m.put(ColumnFamilyConstants.COLF_I, new Text(triangle), value);
            m.put(ColumnFamilyConstants.COLF_I, new Text(quadrilateral), value);
            m.put(ColumnFamilyConstants.COLF_I, new Text(pentagon), value);
            m.put(ColumnFamilyConstants.COLF_I, new Text(hexagon), value);
            m.put(ColumnFamilyConstants.COLF_I, new Text(octagon), value);

            m.put(ColumnFamilyConstants.COLF_RI, new Text(triangle), value);
            m.put(ColumnFamilyConstants.COLF_RI, new Text(quadrilateral), value);
            m.put(ColumnFamilyConstants.COLF_RI, new Text(pentagon), value);
            m.put(ColumnFamilyConstants.COLF_RI, new Text(hexagon), value);
            m.put(ColumnFamilyConstants.COLF_RI, new Text(octagon), value);

            m.put(ColumnFamilyConstants.COLF_T, new Text(triangle + "\0" + normalizerForField("SHAPE")), value);
            m.put(ColumnFamilyConstants.COLF_T, new Text(quadrilateral + "\0" + normalizerForField("SHAPE")), value);
            m.put(ColumnFamilyConstants.COLF_T, new Text(pentagon + "\0" + normalizerForField("SHAPE")), value);
            m.put(ColumnFamilyConstants.COLF_T, new Text(hexagon + "\0" + normalizerForField("SHAPE")), value);
            m.put(ColumnFamilyConstants.COLF_T, new Text(octagon + "\0" + normalizerForField("SHAPE")), value);
            bw.addMutation(m);

            // TYPE has events (e), frequency (f), is indexed (i), and is reverse indexed (ri)
            m = new Mutation("TYPE");
            m.put(ColumnFamilyConstants.COLF_E, new Text(triangle), value);
            m.put(ColumnFamilyConstants.COLF_E, new Text(quadrilateral), value);
            m.put(ColumnFamilyConstants.COLF_E, new Text(pentagon), value);
            m.put(ColumnFamilyConstants.COLF_E, new Text(hexagon), value);
            m.put(ColumnFamilyConstants.COLF_E, new Text(octagon), value);

            m.put(ColumnFamilyConstants.COLF_F, new Text(triangle + '\u0000' + shard), createValue(10L));
            m.put(ColumnFamilyConstants.COLF_F, new Text(quadrilateral + '\u0000' + shard), createValue(14L));
            m.put(ColumnFamilyConstants.COLF_F, new Text(pentagon + '\u0000' + shard), createValue(11L));
            m.put(ColumnFamilyConstants.COLF_F, new Text(hexagon + '\u0000' + shard), createValue(13L));
            m.put(ColumnFamilyConstants.COLF_F, new Text(octagon + '\u0000' + shard), createValue(12L));

            m.put(ColumnFamilyConstants.COLF_I, new Text(triangle), value);
            m.put(ColumnFamilyConstants.COLF_I, new Text(quadrilateral), value);
            m.put(ColumnFamilyConstants.COLF_I, new Text(pentagon), value);
            m.put(ColumnFamilyConstants.COLF_I, new Text(hexagon), value);
            m.put(ColumnFamilyConstants.COLF_I, new Text(octagon), value);

            m.put(ColumnFamilyConstants.COLF_RI, new Text(triangle), value);
            m.put(ColumnFamilyConstants.COLF_RI, new Text(quadrilateral), value);
            m.put(ColumnFamilyConstants.COLF_RI, new Text(pentagon), value);
            m.put(ColumnFamilyConstants.COLF_RI, new Text(hexagon), value);
            m.put(ColumnFamilyConstants.COLF_RI, new Text(octagon), value);

            m.put(ColumnFamilyConstants.COLF_T, new Text(triangle + "\0" + normalizerForField("TYPE")), value);
            m.put(ColumnFamilyConstants.COLF_T, new Text(quadrilateral + "\0" + normalizerForField("TYPE")), value);
            m.put(ColumnFamilyConstants.COLF_T, new Text(pentagon + "\0" + normalizerForField("TYPE")), value);
            m.put(ColumnFamilyConstants.COLF_T, new Text(hexagon + "\0" + normalizerForField("TYPE")), value);
            m.put(ColumnFamilyConstants.COLF_T, new Text(octagon + "\0" + normalizerForField("TYPE")), value);
            bw.addMutation(m);

            m = new Mutation("EDGES");
            m.put(ColumnFamilyConstants.COLF_E, new Text(triangle), value);
            m.put(ColumnFamilyConstants.COLF_E, new Text(quadrilateral), value);
            m.put(ColumnFamilyConstants.COLF_E, new Text(pentagon), value);
            m.put(ColumnFamilyConstants.COLF_E, new Text(hexagon), value);
            m.put(ColumnFamilyConstants.COLF_E, new Text(octagon), value);

            m.put(ColumnFamilyConstants.COLF_I, new Text(triangle), value);
            m.put(ColumnFamilyConstants.COLF_I, new Text(quadrilateral), value);
            m.put(ColumnFamilyConstants.COLF_I, new Text(pentagon), value);
            m.put(ColumnFamilyConstants.COLF_I, new Text(hexagon), value);
            m.put(ColumnFamilyConstants.COLF_I, new Text(octagon), value);

            m.put(ColumnFamilyConstants.COLF_T, new Text(triangle + "\0" + normalizerForField("EDGES")), value);
            m.put(ColumnFamilyConstants.COLF_T, new Text(quadrilateral + "\0" + normalizerForField("EDGES")), value);
            m.put(ColumnFamilyConstants.COLF_T, new Text(pentagon + "\0" + normalizerForField("EDGES")), value);
            m.put(ColumnFamilyConstants.COLF_T, new Text(hexagon + "\0" + normalizerForField("EDGES")), value);
            m.put(ColumnFamilyConstants.COLF_T, new Text(octagon + "\0" + normalizerForField("EDGES")), value);
            bw.addMutation(m);

            // ONLY_*
            m = new Mutation("ONLY_TRI");
            m.put(ColumnFamilyConstants.COLF_I, new Text(triangle), value);
            m.put(ColumnFamilyConstants.COLF_T, new Text(triangle + "\0" + normalizerForField("ONLY_TRI")), value);
            bw.addMutation(m);
            m = new Mutation("ONLY_QUAD");
            m.put(ColumnFamilyConstants.COLF_I, new Text(quadrilateral), value);
            m.put(ColumnFamilyConstants.COLF_T, new Text(quadrilateral + "\0" + normalizerForField("ONLY_QUAD")), value);
            bw.addMutation(m);
            m = new Mutation("ONLY_PENTA");
            m.put(ColumnFamilyConstants.COLF_I, new Text(pentagon), value);
            m.put(ColumnFamilyConstants.COLF_T, new Text(pentagon + "\0" + normalizerForField("ONLY_PENTA")), value);
            bw.addMutation(m);
            m = new Mutation("ONLY_HEX");
            m.put(ColumnFamilyConstants.COLF_I, new Text(hexagon), value);
            m.put(ColumnFamilyConstants.COLF_T, new Text(hexagon + "\0" + normalizerForField("ONLY_HEX")), value);
            bw.addMutation(m);
            m = new Mutation("ONLY_OCT");
            m.put(ColumnFamilyConstants.COLF_I, new Text(octagon), value);
            m.put(ColumnFamilyConstants.COLF_T, new Text(octagon + "\0" + normalizerForField("ONLY_OCT")), value);
            bw.addMutation(m);
        }

        // TODO -- query model
    }

    private static void tokenize(AccumuloClient client, BatchWriterConfig config, String field, String data, RangeType type, String datatype, String uid)
                    throws Exception {
        tokenize(client, config, field, data, uid, type, datatype, false);
    }

    private static void tokenize(AccumuloClient client, BatchWriterConfig config, String field, String data, String uid, RangeType type, String datatype,
                    boolean indexOnly) throws Exception {
        List<String> tokens = list.normalizeToMany(data);

        // write shard index tokens
        try (BatchWriter bw = client.createBatchWriter(SHARD_INDEX, config)) {
            for (String token : tokens) {
                Mutation m = new Mutation(token);
                m.put(field, shard + '\u0000' + datatype, cv, ts, getValue(type, uid));
                bw.addMutation(m);
            }
        }

        // write field index and TF tokens
        try (BatchWriter bw = client.createBatchWriter(SHARD, config)) {
            Mutation m = new Mutation(shard);
            for (String token : tokens) {
                m.put("fi\0" + field, token + '\u0000' + datatype + "\0" + uid, cv, ts, value);
            }

            // write TF tokens
            for (String token : tokens) {
                // should build a correct TF value for content function queries
                m.put("tf", datatype + "\0" + uid + "\0" + token + "\0" + field, cv, ts, value);
            }

            // write event data, if requested
            if (!indexOnly) {
                m.put(datatype + "\0" + uid, field + "\0" + data, cv, ts, value);
            }
            bw.addMutation(m);
        }
    }

    private static Value getValue(RangeType type, String uid) {
        Uid.List.Builder builder = Uid.List.newBuilder();
        if (type.equals(RangeType.DOCUMENT)) {
            builder.setIGNORE(false);
            builder.setCOUNT(1L);
            builder.addUID(uid);
        } else {
            builder.setIGNORE(true);
            builder.setCOUNT(17L); // arbitrary prime number below the 20 max uid limit
        }
        return new Value(builder.build().toByteArray());
    }

    private static Value createValue(long count) {
        return new Value(encoder.encode(count));
    }
}

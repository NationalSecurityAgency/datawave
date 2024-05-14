package datawave.ingest.mapreduce.job;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.accumulo.core.data.Key;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

public class LocalityGroupBulkIngestKeyComparatorTest {
    private final static int COMP_EQ = 0;
    private final static int COMP_LT = -1;
    private final static int COMP_GT = 1;

    @ParameterizedTest
    @MethodSource("data")
    public void testComparisons(int expectedResult, Object[] k1, Object[] k2, String[] lg) throws Exception {
        var comparator = new LocalityGroupBulkIngestKeyComparator();
        var lgConf = Mockito.mock(LocalityGroupConfiguration.class);
        if (lg == null) {
            Mockito.when(lgConf.getLocalityGroups((String) k1[1])).thenReturn(Map.of());
        } else {
            var lgMap = new HashMap<String,Set<Text>>();
            Arrays.stream(lg).forEach(v -> {
                var lgSplit = v.split("=");
                var lgColSet = Set.<Text> of();
                if (lgSplit.length > 1) {
                    lgColSet = Arrays.stream(lgSplit[1].split(",")).map(Text::new).collect(Collectors.toSet());
                }
                lgMap.put(lgSplit[0], lgColSet);
            });
            Mockito.when(lgConf.getLocalityGroups((String) k1[0])).thenReturn(lgMap);
            Mockito.when(lgConf.getLocalityGroups((String) k2[0])).thenReturn(lgMap);
        }

        comparator.setLocalityGroupConfiguration(lgConf);

        var b1 = newKeyBytes((String) k1[0], (String) k1[1], (String) k1[2], (String) k1[3], (String) k1[4], (Long) k1[5]);
        var b2 = newKeyBytes((String) k2[0], (String) k2[1], (String) k2[2], (String) k2[3], (String) k2[4], (Long) k2[5]);
        assertEquals(expectedResult, comparator.compare(b1, 0, b1.length, b2, 0, b2.length));
    }

    static Stream<Arguments> data() {
        return Stream.of(
                        Arguments.arguments(COMP_EQ, new Object[] {"t1", "r1", "cf1", "cq1", "cv", 0L}, new Object[] {"t1", "r1", "cf1", "cq1", "cv", 0L},
                                        new String[] {"lg"}),
                        Arguments.arguments(COMP_LT, new Object[] {"t1", "r1", "cf1", "cq1", "cv", 0L}, new Object[] {"t2", "r1", "cf1", "cq1", "cv", 0L},
                                        new String[] {"lg"}),
                        Arguments.arguments(COMP_LT, new Object[] {"t1", "r1", "cf1", "cq1", "cv", 0L}, new Object[] {"t1", "r2", "cf1", "cq1", "cv", 0L},
                                        new String[] {"lg"}),
                        Arguments.arguments(COMP_LT, new Object[] {"t1", "r1", "cf1", "cq1", "cv", 0L}, new Object[] {"t1", "r1", "cf2", "cq1", "cv", 0L},
                                        new String[] {"lg"}),
                        Arguments.arguments(COMP_LT, new Object[] {"t1", "r1", "cf1", "cq1", "cv", 0L}, new Object[] {"t1", "r1", "cf1", "cq2", "cv", 0L},
                                        new String[] {"lg"}),
                        Arguments.arguments(COMP_LT, new Object[] {"t1", "r1", "cf1", "cq1", "cv", 0L}, new Object[] {"t1", "r1", "cf1", "cq1", "cv1", 0L},
                                        new String[] {"lg"}),

                        // lg cf2 and default lg - cf2 should be sorted before cf1
                        Arguments.arguments(COMP_LT, new Object[] {"t1", "r1", "cf2", "cq1", "cv", 0L}, new Object[] {"t1", "r1", "cf1", "cq1", "cv", 0L},
                                        new String[] {"lg=cf2"}),

                        // default lg and lg cf1 - cf2 should be sorted after cf1
                        Arguments.arguments(COMP_GT, new Object[] {"t1", "r1", "cf2", "cq1", "cv", 0L}, new Object[] {"t1", "r1", "cf1", "cq1", "cv", 0L},
                                        new String[] {"lg=cf1"})

        );
    }

    private static byte[] newKeyBytes(String table, String row, String cf, String cq, String cv, long timestamp) {
        var k = newKey(table, row, cf, cq, cv, timestamp);
        return toBytes(k);
    }

    private static BulkIngestKey newKey(String table, String row, String cf, String cq, String cv, long ts) {
        return new BulkIngestKey(new Text(table), Key.builder().row(row).family(cf).qualifier(cq).visibility(cv).timestamp(ts).build());
    }

    private static byte[] toBytes(BulkIngestKey key) {
        try (var bos = new ByteArrayOutputStream(); var dos = new DataOutputStream(bos)) {
            key.write(dos);
            dos.flush();
            return bos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}

package datawave.query.analysis;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;

import org.junit.Test;

import datawave.query.analysis.QueryAnalyzer.Syntax;

public class QueryTuningConfigurationTest {
    @Test
    public void samplePreservesRawTextAndLabels() {
        QueryTuningSample sample = new QueryTuningSample(" id ", "  NAME:alice  ", Syntax.LUCENE, " bucket ");
        assertEquals(" id ", sample.getId());
        assertEquals("  NAME:alice  ", sample.getQuery());
        assertEquals(Syntax.LUCENE, sample.getSyntax());
        assertEquals(" bucket ", sample.getBucket());
        assertEquals(sample.getQuery(), sample.toQueryInput().getQuery());
        assertEquals(sample.getSyntax(), sample.toQueryInput().getSyntax());
        assertNull(new QueryTuningSample("A == 1", Syntax.JEXL, "a").getId());
    }

    @Test
    public void sampleRequiresNonblankFieldsAndExplicitSyntax() {
        for (String blank : new String[] {null, "", " \t\n", "\u2003"}) {
            assertThrows(IllegalArgumentException.class, () -> new QueryTuningSample(blank, Syntax.JEXL, "a"));
            assertThrows(IllegalArgumentException.class, () -> new QueryTuningSample("A == 1", Syntax.JEXL, blank));
            if (blank != null) {
                assertThrows(IllegalArgumentException.class, () -> new QueryTuningSample(blank, "A == 1", Syntax.JEXL, "a"));
            }
        }
        assertThrows(NullPointerException.class, () -> new QueryTuningSample("A == 1", null, "a"));
    }

    @Test
    public void configurationRetainsImmutableClusteringOptionsAndVersions() {
        QueryClusterer.Options options = new QueryClusterer.Options(0.81, 7, 19, 47, new QuerySimilarity.Weights(8, 2, 1, 0));
        QueryTuningConfiguration configuration = new QueryTuningConfiguration(options);
        assertEquals(1, configuration.getSchemaVersion());
        assertEquals(QueryFingerprint.VERSION, configuration.getFingerprintVersion());
        assertSame(options, configuration.toClusteringOptions());
        assertThrows(NullPointerException.class, () -> new QueryTuningConfiguration(null));
    }
}

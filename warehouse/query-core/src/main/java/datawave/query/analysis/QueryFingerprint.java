package datawave.query.analysis;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Versioned, immutable syntactic planning features. A protected profile is a conservative grouping boundary, not a prediction of the actual execution plan.
 * Ordinary search values are abstracted; regex access characteristics and known function controls are retained.
 */
public final class QueryFingerprint {
    public static final int VERSION = 2;

    private final String signature;
    private final String key;
    private final String protectedProfile;
    private final Set<String> bindings;
    private final Map<String,Integer> counts;
    private final Set<String> topology;
    private final Map<String,Double> measurements;
    private final Set<String> protections;
    private final List<String> diagnostics;

    QueryFingerprint(String signature, String protectedProfile, Set<String> bindings, Map<String,Integer> counts, Set<String> topology,
                    Map<String,Double> measurements, Set<String> protections, List<String> diagnostics) {
        this.signature = signature;
        this.protectedProfile = protectedProfile;
        this.bindings = Collections.unmodifiableSet(new TreeSet<>(bindings));
        this.counts = Collections.unmodifiableMap(new TreeMap<>(counts));
        this.topology = Collections.unmodifiableSet(new TreeSet<>(topology));
        this.measurements = Collections.unmodifiableMap(new TreeMap<>(measurements));
        this.protections = Collections.unmodifiableSet(new TreeSet<>(protections));
        this.diagnostics = Collections.unmodifiableList(new ArrayList<>(diagnostics));
        this.key = "qf2:" + digest(encode("fingerprint", List.of(signature, protectedProfile)));
    }

    public int getVersion() {
        return VERSION;
    }

    public String getSignature() {
        return signature;
    }

    public String getKey() {
        return key;
    }

    public String getProtectedProfile() {
        return protectedProfile;
    }

    public Set<String> getBindings() {
        return bindings;
    }

    public Map<String,Integer> getCounts() {
        return counts;
    }

    public Set<String> getTopology() {
        return topology;
    }

    public Map<String,Double> getMeasurements() {
        return measurements;
    }

    public Set<String> getProtections() {
        return protections;
    }

    public List<String> getDiagnostics() {
        return diagnostics;
    }

    static String encode(String label, Collection<String> children) {
        StringBuilder result = new StringBuilder().append(label.length()).append(':').append(label).append('(');
        for (String child : children) {
            result.append(child.length()).append(':').append(child);
        }
        return result.append(')').toString();
    }

    static String inputKey(QueryAnalyzer.QueryAnalysis query) {
        return encode(query.getInput().getSyntax().name(), List.of(query.getInput().getQuery()));
    }

    static String digest(String value) {
        try {
            byte[] bytes = MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8));
            char[] hex = "0123456789abcdef".toCharArray();
            StringBuilder result = new StringBuilder(64);
            for (byte b : bytes) {
                result.append(hex[(b & 255) >>> 4]).append(hex[b & 15]);
            }
            return result.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is required by the Java platform", e);
        }
    }
}

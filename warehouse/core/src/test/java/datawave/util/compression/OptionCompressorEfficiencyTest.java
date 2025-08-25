package datawave.util.compression;

import datawave.query.util.TypeMetadata;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.StringUtils; // Apache Commons Lang for capitalization
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Compression benchmark for OptionCompressor across generated datasets.
 *
 * - Prints a concise summary table per run (averages over MEASUREMENT_TRIALS)
 * - Optional: dumps the full corpus mapping (fields -> types -> ingests) at the END of each run
 * - Optional: runs multiple exponentially larger datasets to observe scaling trends
 */
public class OptionCompressorEfficiencyTest {

    private static final Charset UTF8 = StandardCharsets.UTF_8;

    // Fixed type set (as requested)
    private static final List<String> TYPES = Arrays.asList(
            "LcNoDiacriticsType", "NumberType", "DateType", "BooleanType", "GeoType"
    );

    // ---- Output toggles ----
    private static final boolean PRINT_SUMMARY_TABLE   = true; // human-readable summary
    private static final boolean PRINT_CORPUS_DETAILS  = false; // single toggle: dump full corpus at end of each run

    // ---- Benchmark configuration ----
    private static final int  MEASUREMENT_TRIALS = 5;     // number of timing iterations (averaged)
    private static final long RNG_SEED           = 42L;   // base seed for deterministic generation

    // ---- Dataset generation (base) ----
    private static final int BASE_FIELD_COUNT    = 64;    // fields generated with natural names
    private static final int BASE_INGEST_COUNT   = 16;     // ingests generated with natural names
    private static final int MAX_TYPES_PER_FIELD = 5;     // chosen from TYPES per field

    // ---- Multi-run exponential scaling ----
    // If enabled, we run SCALE_STEPS datasets. Each step multiplies fields & ingests by SCALE_FACTOR^step.
    private static final boolean SCALE_CORPUS_SIZES = true;
    private static final int     SCALE_STEPS        = 3;  // e.g., 4 runs: base, x2, x4, x8 (with factor 2)
    private static final int     SCALE_FACTOR       = 4;  // >=1; 2 = doubles each step

    // Word roots for natural, number-free names
    private static final String[] NAME_WORDS = {
            "lorem","ipsum","dolor","amet","terra","orbis","nova","astra","luna","sol","nimbus","cirrus","zenith","apex",
            "aurora","boreal","ember","aqua","flumen","rivus","silva","folium","radix","vertex","praxis","nexus","vector",
            "argon","neon","ion","quanta","quantum","plasma","flux","sigma","omega","delta","theta","lambda","kappa","alpha",
            "gamma","zeta","rho","tau","psi","mercury","atlas","phoenix","orion","vega","sirius","altair","pangea","cedrus",
            "cortex","lumen","umbra","umbrel","vulcan","cronos","helios","gaia","aether","strata","tundra","sylvan","arbor",
            "granum","cumulus","stratus","horizon","axiom","lemma","theorem","matrix","vectora","volt","ampere"
    };

    @Test
    void runCompressionBenchmarkSuite() throws IOException {
        OptionCompressor compressor = new OptionCompressor();
        List<BenchmarkDataset> datasets = new ArrayList<>();

        if (SCALE_CORPUS_SIZES && SCALE_STEPS > 0 && SCALE_FACTOR >= 1) {
            for (int step = 0; step < SCALE_STEPS; step++) {
                long stepSeed = RNG_SEED + step * 1337L; // distinct, deterministic seed per step
                int scale     = intPow(SCALE_FACTOR, step);
                int fields    = Math.max(1, BASE_FIELD_COUNT  * scale);
                int ingests   = Math.max(1, BASE_INGEST_COUNT * scale);

                String runName = String.format(Locale.ROOT,
                        "generated(step=%d,fields=%d,ingests=%d,maxTypes=%d)",
                        step, fields, ingests, MAX_TYPES_PER_FIELD);

                datasets.add(buildDataset(runName, fields, ingests, MAX_TYPES_PER_FIELD, TYPES, stepSeed));
            }
        } else {
            datasets.add(buildDataset(
                    "generated(base)",
                    BASE_FIELD_COUNT, BASE_INGEST_COUNT, MAX_TYPES_PER_FIELD, TYPES, RNG_SEED
            ));
        }

        // Warm up the JVM/compressor once across all datasets & algorithms
        warmupAllMethods(compressor, datasets);

        if (PRINT_SUMMARY_TABLE) {
            printSummaryLegend(MEASUREMENT_TRIALS);
        }

        // ---- Main measurement loop ----
        for (BenchmarkDataset dataset : datasets) {
            final byte[] originalBytes = dataset.serialized.getBytes(UTF8);
            final int    originalLen   = originalBytes.length;

            if (PRINT_SUMMARY_TABLE) {
                printRunHeader(dataset.name, originalLen);
                printSummaryHeader();
            }

            for (OptionCompressor.CompressionMethod method : OptionCompressor.CompressionMethod.values()) {
                long compressNanosTotal   = 0;
                long decompressNanosTotal = 0;
                int  compressedLenRaw     = 0;
                int  compressedLenB64     = 0;

                String lastEncoded = null;

                for (int i = 0; i < MEASUREMENT_TRIALS; i++) {
                    long t0 = System.nanoTime();
                    lastEncoded = compressor.compress(dataset.serialized, method, UTF8);
                    long t1 = System.nanoTime();
                    compressNanosTotal += (t1 - t0);

                    if (method == OptionCompressor.CompressionMethod.NONE) {
                        compressedLenRaw = originalLen;
                        compressedLenB64 = originalLen;
                    } else {
                        byte[] decoded = Base64.decodeBase64(lastEncoded);
                        compressedLenRaw = decoded.length;
                        compressedLenB64 = lastEncoded.getBytes(UTF8).length;
                    }

                    long t2 = System.nanoTime();
                    String restored = compressor.decompress(lastEncoded, method, UTF8);
                    long t3 = System.nanoTime();
                    decompressNanosTotal += (t3 - t2);

                    assertEquals(dataset.serialized, restored, "Round-trip must match for " + method);
                }

                double avgCompressMs   = nanosToMillis(compressNanosTotal   / (double) MEASUREMENT_TRIALS);
                double avgDecompressMs = nanosToMillis(decompressNanosTotal / (double) MEASUREMENT_TRIALS);
                double ratioRaw        = (double) compressedLenRaw / originalLen;
                double ratioB64        = (double) compressedLenB64 / originalLen;

                if (PRINT_SUMMARY_TABLE) {
                    printSummaryRow(
                            method.name(),
                            humanBytes(originalLen),
                            humanBytes(compressedLenRaw),
                            humanBytes(compressedLenB64),
                            percentSaved(ratioRaw),
                            percentSaved(ratioB64),
                            avgCompressMs,
                            avgDecompressMs
                    );
                }
            }

            // ---- Dump the full corpus mapping at the END of this run (single boolean) ----
            if (PRINT_CORPUS_DETAILS) {
                printCorpusDetails(dataset);
            }

            if (PRINT_SUMMARY_TABLE) {
                System.out.println(); // spacer between runs
            }
        }
    }

    // ------------------------ Naming & formatting helpers ------------------------

    private static void printSummaryLegend(int trials) {
        System.out.println();
        System.out.println("OptionCompressor Benchmark — averages over " + trials + " trials (lower time is better; higher % saved is better).");
        System.out.println("Columns:");
        System.out.println("  Method         Compression algorithm");
        System.out.println("  Original       Original size");
        System.out.println("  Compressed     Size after compression (no Base64)");
        System.out.println("  Base64         Size after compression + Base64 text");
        System.out.println("  Saved (raw)    % space saved vs original (no Base64)");
        System.out.println("  Saved (b64)    % space saved vs original (with Base64)");
        System.out.println("  Compress/Decompress: average milliseconds");
        System.out.println();
    }

    private static void printRunHeader(String name, int origLenBytes) {
        System.out.printf("Run: %s  |  Original total: %s (%d bytes)%n", name, humanBytes(origLenBytes), origLenBytes);
    }

    private static void printSummaryHeader() {
        System.out.printf("%-14s  %-11s  %-11s  %-10s  %-11s  %-13s%n",
                "Method", "Original", "Compressed", "Base64", "Saved (raw)", "Saved (b64)");
        System.out.printf("%-14s  %-11s  %-11s  %-10s  %-11s  %-13s%n",
                repeat('-',14), repeat('-',11), repeat('-',11), repeat('-',10), repeat('-',11), repeat('-',13));
    }

    private static void printSummaryRow(String method,
                                        String original,
                                        String compressed,
                                        String base64,
                                        String savedRawPct,
                                        String savedB64Pct,
                                        double compressMs,
                                        double decompressMs) {
        System.out.printf("%-14s  %-11s  %-11s  %-10s  %-11s  %-13s  |  Compress: %6.3f ms  Decompress: %6.3f ms%n",
                method, original, compressed, base64, savedRawPct, savedB64Pct, compressMs, decompressMs);
    }

    private static String humanBytes(long bytes) {
        final String[] units = {"B","KiB","MiB","GiB","TiB"};
        double v = bytes;
        int u = 0;
        while (v >= 1024.0 && u < units.length - 1) { v /= 1024.0; u++; }
        if (u == 0) return String.format(Locale.ROOT, "%d%s", bytes, units[u]);
        return String.format(Locale.ROOT, "%.2f%s", v, units[u]);
    }

    private static String percentSaved(double ratioCompressedOverOriginal) {
        double saved = (1.0 - ratioCompressedOverOriginal) * 100.0;
        return String.format(Locale.ROOT, "%.1f%%", saved);
    }

    private static String repeat(char c, int n) {
        char[] arr = new char[n];
        Arrays.fill(arr, c);
        return new String(arr);
    }

    private static double nanosToMillis(double nanos) {
        return nanos / 1_000_000.0;
    }

    private static int intPow(int base, int exp) {
        int r = 1;
        for (int i = 0; i < exp; i++) r = Math.multiplyExact(r, base);
        return r;
    }

    // ------------------------ Corpus dump (single boolean; no preview tuning) ------------------------

    private static void printCorpusDetails(BenchmarkDataset ds) {
        System.out.printf("%n=== Corpus Details ===%n");
        System.out.printf("Seed: %d%n", ds.seed);
        System.out.printf("Fields: %d   Ingests: %d   MaxTypesPerField: %d   TypesUsed: %d / %d   Triples: %,d%n",
                ds.generatedFieldCount, ds.generatedIngestCount, ds.maxTypesPerField,
                ds.distinctTypes.size(), ds.totalTypesAvailable, ds.triples.size());

        // Full, untruncated listing (sorted for determinism)
        List<String> fields = new ArrayList<>(ds.byField.keySet());
        Collections.sort(fields);
        for (String f : fields) {
            System.out.println("- " + f + ":");
            Map<String, Set<String>> types = ds.byField.getOrDefault(f, Collections.emptyMap());
            List<String> typeNames = new ArrayList<>(types.keySet());
            Collections.sort(typeNames);
            for (String t : typeNames) {
                List<String> ing = new ArrayList<>(types.getOrDefault(t, Collections.emptySet()));
                Collections.sort(ing);
                System.out.println("    " + t + " -> " + String.join(", ", ing));
            }
        }
        System.out.println("======================\n");
    }

    // ------------------------ Dataset generation & warmup ------------------------

    private static class Mapping {
        final String field, ingest, type;
        Mapping(String f, String i, String t) { this.field = f; this.ingest = i; this.type = t; }
    }

    private static class BenchmarkDataset {
        final String name;
        final String serialized; // TypeMetadata serialization
        final long   seed;

        final int generatedFieldCount;
        final int generatedIngestCount;
        final int maxTypesPerField;
        final int totalTypesAvailable;

        final List<Mapping> triples;

        final Set<String> distinctFields = new HashSet<>();
        final Set<String> distinctIngests = new HashSet<>();
        final Set<String> distinctTypes   = new HashSet<>();
        // field -> type -> ingests
        final Map<String, Map<String, Set<String>>> byField = new HashMap<>();

        BenchmarkDataset(String name,
                         String serialized,
                         long seed,
                         int generatedFieldCount,
                         int generatedIngestCount,
                         int maxTypesPerField,
                         int totalTypesAvailable,
                         List<Mapping> triples) {
            this.name = name;
            this.serialized = serialized;
            this.seed = seed;
            this.generatedFieldCount = generatedFieldCount;
            this.generatedIngestCount = generatedIngestCount;
            this.maxTypesPerField = maxTypesPerField;
            this.totalTypesAvailable = totalTypesAvailable;
            this.triples = triples;

            for (Mapping m : triples) {
                distinctFields.add(m.field);
                distinctIngests.add(m.ingest);
                distinctTypes.add(m.type);
                byField
                        .computeIfAbsent(m.field, k -> new HashMap<>())
                        .computeIfAbsent(m.type,  k -> new HashSet<>())
                        .add(m.ingest);
            }
        }
    }

    private static void warmupAllMethods(OptionCompressor compressor, List<BenchmarkDataset> datasets) {
        for (BenchmarkDataset ds : datasets) {
            for (OptionCompressor.CompressionMethod m : OptionCompressor.CompressionMethod.values()) {
                try {
                    String enc = compressor.compress(ds.serialized, m, UTF8);
                    String dec = compressor.decompress(enc, m, UTF8);
                    assertEquals(ds.serialized, dec);
                } catch (Exception ignored) {}
            }
        }
    }

    /**
     * Build a dataset from generated, natural (number-free) field/ingest names and
     * a deterministic random subset (up to maxTypesPerField) of TYPES per field.
     * For each (field, chosenType), ALL ingests are included.
     */
    private static BenchmarkDataset buildDataset(String name,
                                                 int fieldCount,
                                                 int ingestCount,
                                                 int maxTypesPerField,
                                                 List<String> availableTypes,
                                                 long seed) {
        if (fieldCount < 1) fieldCount = 1;
        if (ingestCount < 1) ingestCount = 1;
        if (maxTypesPerField < 1) maxTypesPerField = 1;

        Random rnd = new Random(seed);

        // Use different derived seeds so fields/ingests differ
        List<String> fields  = generateNaturalNames(fieldCount,  new Random(seed ^ 0xC0FFEE1234ABCDEFL));
        List<String> ingests = generateNaturalNames(ingestCount, new Random(seed ^ 0xBEEFBABE56789ABCL));

        TypeMetadata tm = new TypeMetadata();
        List<Mapping> triples = new ArrayList<>();

        for (String f : fields) {
            List<String> shuffled = new ArrayList<>(availableTypes);
            Collections.shuffle(shuffled, rnd);
            int choose = Math.min(maxTypesPerField, shuffled.size());
            List<String> chosenTypes = shuffled.subList(0, choose);

            for (String t : chosenTypes) {
                for (String i : ingests) {
                    tm.put(f, i, t);
                    triples.add(new Mapping(f, i, t));
                }
            }
        }

        return new BenchmarkDataset(
                name,
                tm.toString(),
                seed,
                fieldCount,
                ingestCount,
                maxTypesPerField,
                availableTypes.size(),
                triples
        );
    }

    /**
     * Generate 'count' unique, natural-looking, number-free names using pairs (then triples) of roots.
     * Deterministic given the Random. Produces lowerCamelCase tokens like "auroraFlux".
     * Uses Apache Commons Lang for capitalization.
     */
    private static List<String> generateNaturalNames(int count, Random rnd) {
        List<String> words = Arrays.asList(NAME_WORDS);

        // All distinct 2-word combinations (order matters; no duplicates like (i,i))
        List<int[]> pairs = new ArrayList<>(words.size() * (words.size() - 1));
        for (int i = 0; i < words.size(); i++) {
            for (int j = 0; j < words.size(); j++) {
                if (i == j) continue;
                pairs.add(new int[]{i, j});
            }
        }
        Collections.shuffle(pairs, rnd);

        LinkedHashSet<String> out = new LinkedHashSet<>(count);
        int p = 0;
        while (out.size() < count && p < pairs.size()) {
            int[] ij = pairs.get(p++);
            out.add(toLowerCamel(words.get(ij[0]), words.get(ij[1])));
        }

        // Top up with 3-word combinations if needed
        if (out.size() < count) {
            List<int[]> triples = new ArrayList<>(words.size() * words.size() * words.size());
            for (int i = 0; i < words.size(); i++) {
                for (int j = 0; j < words.size(); j++) {
                    if (i == j) continue;
                    for (int k = 0; k < words.size(); k++) {
                        if (k == i || k == j) continue;
                        triples.add(new int[]{i, j, k});
                    }
                }
            }
            Collections.shuffle(triples, rnd);
            int t = 0;
            while (out.size() < count && t < triples.size()) {
                int[] ijk = triples.get(t++);
                out.add(toLowerCamel(words.get(ijk[0]), words.get(ijk[1]), words.get(ijk[2])));
            }
        }

        // Safety fallback
        if (out.size() < count) {
            for (String w : words) {
                out.add(toLowerCamel(w));
                if (out.size() >= count) break;
            }
        }
        return new ArrayList<>(out).subList(0, count);
    }

    private static String toLowerCamel(String... parts) {
        if (parts == null || parts.length == 0) return "";
        List<String> clean = new ArrayList<>(parts.length);
        for (String p : parts) {
            if (p == null) continue;
            String s = p.replaceAll("[^A-Za-z]", "").toLowerCase(Locale.ROOT);
            if (!s.isEmpty()) clean.add(s);
        }
        if (clean.isEmpty()) return "";
        StringBuilder sb = new StringBuilder(clean.get(0)); // first stays lower
        for (int i = 1; i < clean.size(); i++) {
            sb.append(StringUtils.capitalize(clean.get(i))); // Apache Commons Lang
        }
        return sb.toString();
    }
}

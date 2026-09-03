package datawave.ingest.table.volumeChoosers;

import static java.time.temporal.ChronoUnit.DAYS;

import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.spi.common.ServiceEnvironment;
import org.apache.accumulo.core.spi.fs.RandomVolumeChooser;
import org.apache.accumulo.core.spi.fs.VolumeChooserEnvironment;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.hash.HashCode;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.google.common.primitives.UnsignedBytes;

//@formatter:off
/**
 * A date-based volume chooser for sharded tables. Tiers are configured with table custom properties. For example:
 *
 * <pre>
 * table.custom.volume.tier.names=hot,cold
 * table.custom.volume.tiered.hot.days.back=0
 * table.custom.volume.tiered.hot.volumes=hdfs://accumulo-a/accumulo,hdfs://accumulo-b/accumulo
 * table.custom.volume.tiered.cold.days.back=125
 * table.custom.volume.tiered.cold.volumes=hdfs://archive-a/accumulo,hdfs://archive-b/accumulo
 * table.custom.volume.tiered.placement=rendezvous
 * </pre>
 *
 * The placement property accepts {@code random} (the default, preserving historical behavior) or {@code rendezvous}. Rendezvous placement guarantees one
 * deterministic volume per tablet within its active tier while the eligible volume set and the hash algorithm remain unchanged. The persisted placement
 * contract is the greatest unsigned Murmur3-128 hash of a versioned, length-prefixed encoding of the table ID, raw end-row bytes (including a distinct null
 * marker), and exact volume URI; equal hashes are resolved by choosing the lexicographically smallest URI.
 *
 * A tier transition can place later files on one volume in the new tier, and changing tier membership can remap tablets (rendezvous hashing minimizes that
 * remapping). Splits and merges also change tablet identities. Existing files or duplicate directories are not migrated or removed.
 *
 * When Accumulo's {@code DelegatingChooser} is used, select this chooser with:
 *
 * <pre>
 * table.custom.volume.chooser=datawave.ingest.table.volumeChoosers.ShardedTableDateBasedTieredVolumeChooser
 * </pre>
 */
//@formatter:on
public class ShardedTableDateBasedTieredVolumeChooser extends RandomVolumeChooser {
    private static final Logger log = LoggerFactory.getLogger(ShardedTableDateBasedTieredVolumeChooser.class);
    private static final String TIER_NAMES_SUFFIX = "volume.tier.names";
    private static final String PROPERTY_PREFIX = "volume.tiered.";
    private static final String VOLUME_SUFFIX = ".volumes";
    private static final String DAYS_BACK_SUFFIX = ".days.back";
    private static final String PLACEMENT_SUFFIX = "volume.tiered.placement";
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd");

    private static final Pattern SHARD_PATTERN = Pattern.compile("\\d{8}_\\d+");
    private static final Pattern SHARD_PATTERN_NO_SUFFIX = Pattern.compile("\\d{8}");

    private enum PlacementStrategy {
        RANDOM, RENDEZVOUS
    }

    private static class Tier {
        private final String name;
        private final Set<String> volumes;

        private Tier(String name, Set<String> volumes) {
            this.name = name;
            this.volumes = volumes;
        }
    }

    @Override
    public String choose(VolumeChooserEnvironment env, Set<String> options) {
        if (!env.getTable().isPresent() || !env.getChooserScope().equals(VolumeChooserEnvironment.Scope.TABLE)) {
            return super.choose(env, options);
        }

        TableId tableId = env.getTable().get();
        ServiceEnvironment.Configuration tableConfig = env.getServiceEnv().getConfiguration(tableId);
        TreeMap<Long,Tier> daysToTiers = getTiers(tableId, tableConfig, options);
        Text endRow = env.getEndRow();
        Tier selectedTier = selectTier(tableId, endRow, daysToTiers);
        Set<String> eligibleVolumes = validateTierVolumes(tableId, selectedTier, options);

        if (getPlacementStrategy(tableId, tableConfig) == PlacementStrategy.RENDEZVOUS) {
            return chooseRendezvous(tableId, endRow, eligibleVolumes);
        }
        return super.choose(env, eligibleVolumes);
    }

    private static Tier selectTier(TableId tableId, Text endRow, TreeMap<Long,Tier> daysToTiers) {
        Long tierKey = daysToTiers.ceilingKey(0L);
        Tier selectedTier = tierKey == null ? daysToTiers.get(Long.MAX_VALUE) : daysToTiers.get(tierKey);
        if (endRow == null) {
            return selectedTier;
        }

        String endRowString = endRow.toString();
        if (SHARD_PATTERN.matcher(endRowString).matches() || SHARD_PATTERN_NO_SUFFIX.matcher(endRowString).matches()) {
            LocalDate rowDate = LocalDate.parse(endRowString.substring(0, 8), FORMATTER);
            long days = DAYS.between(rowDate, LocalDate.now());
            tierKey = daysToTiers.floorKey(days > 0L ? days : 0L);
            return tierKey == null ? daysToTiers.get(Long.MAX_VALUE) : daysToTiers.get(tierKey);
        }

        log.warn("endRow does not match pattern. Is this table sharded? endRow was {} and tableId is {}", endRowString, tableId);
        return daysToTiers.get(Long.MAX_VALUE);
    }

    private static TreeMap<Long,Tier> getTiers(TableId tableId, ServiceEnvironment.Configuration tableConfig, Set<String> options) {
        TreeMap<Long,Tier> daysToTiers = new TreeMap<>();
        daysToTiers.put(Long.MAX_VALUE, new Tier("Accumulo supplied options", options));
        for (String tier : listTiers(tableId, tableConfig)) {
            String configuredVolumes = getTierProperty(tableId, tableConfig, tier, VOLUME_SUFFIX);
            Set<String> volumesForCurrentTier = configuredVolumes == null ? Set.of()
                            : Arrays.stream(configuredVolumes.split(",", -1)).map(String::trim).filter(volume -> !volume.isEmpty())
                                            .collect(Collectors.toCollection(LinkedHashSet::new));
            long daysBackForCurrentTier = getTierDaysBack(tableId, tableConfig, tier);
            if (daysBackForCurrentTier < 0) {
                throw new IllegalStateException("Invalid days back for tier " + tier + " on table " + tableId + ". Must be >= 0");
            }
            if (volumesForCurrentTier.isEmpty()) {
                throw new IllegalStateException("Volumes list empty for tier " + tier + " on table " + tableId + ". Ensure property "
                                + Property.TABLE_ARBITRARY_PROP_PREFIX + PROPERTY_PREFIX + tier + VOLUME_SUFFIX + " is set");
            }
            daysToTiers.put(daysBackForCurrentTier, new Tier(tier, volumesForCurrentTier));
        }
        return daysToTiers;
    }

    private static Set<String> validateTierVolumes(TableId tableId, Tier tier, Set<String> options) {
        Set<String> invalidVolumes = tier.volumes.stream().filter(volume -> !options.contains(volume)).collect(Collectors.toCollection(LinkedHashSet::new));
        if (!invalidVolumes.isEmpty()) {
            throw new IllegalStateException("Configured volumes are not available for table " + tableId + ", tier " + tier.name + ": " + invalidVolumes
                            + ". Accumulo supplied options: " + options);
        }
        return tier.volumes;
    }

    private static PlacementStrategy getPlacementStrategy(TableId tableId, ServiceEnvironment.Configuration tableConfig) {
        String configuredStrategy = tableConfig.getTableCustom(PLACEMENT_SUFFIX);
        if (configuredStrategy == null || configuredStrategy.trim().isEmpty() || "random".equalsIgnoreCase(configuredStrategy.trim())) {
            return PlacementStrategy.RANDOM;
        }
        if ("rendezvous".equalsIgnoreCase(configuredStrategy.trim())) {
            return PlacementStrategy.RENDEZVOUS;
        }
        throw new IllegalStateException("Invalid placement strategy '" + configuredStrategy + "' for table " + tableId + ". Property "
                        + Property.TABLE_ARBITRARY_PROP_PREFIX + PLACEMENT_SUFFIX + " must be 'random' or 'rendezvous'");
    }

    /**
     * Selects the candidate with the greatest unsigned Murmur3-128 score. The byte format is version 1, followed by length-prefixed UTF-8 table ID, a null
     * marker or length-prefixed raw end row, and a length-prefixed UTF-8 exact volume URI.
     */
    static String chooseRendezvous(TableId tableId, Text endRow, Set<String> volumes) {
        if (volumes.isEmpty()) {
            throw new IllegalStateException("Cannot choose a rendezvous volume from an empty set for table " + tableId);
        }

        byte[] tableBytes = tableId.canonical().getBytes(StandardCharsets.UTF_8);
        String selectedVolume = null;
        HashCode selectedHash = null;
        for (String volume : volumes) {
            byte[] volumeBytes = volume.getBytes(StandardCharsets.UTF_8);
            Hasher hasher = Hashing.murmur3_128().newHasher();
            hasher.putByte((byte) 1).putInt(tableBytes.length).putBytes(tableBytes);
            if (endRow == null) {
                hasher.putByte((byte) 0);
            } else {
                hasher.putByte((byte) 1).putInt(endRow.getLength()).putBytes(endRow.getBytes(), 0, endRow.getLength());
            }
            HashCode candidateHash = hasher.putInt(volumeBytes.length).putBytes(volumeBytes).hash();
            int comparison = selectedHash == null ? 1 : UnsignedBytes.lexicographicalComparator().compare(candidateHash.asBytes(), selectedHash.asBytes());
            if (comparison > 0 || (comparison == 0 && volume.compareTo(selectedVolume) < 0)) {
                selectedVolume = volume;
                selectedHash = candidateHash;
            }
        }
        return selectedVolume;
    }

    public static Set<String> listTiers(TableId tableId, ServiceEnvironment.Configuration tableConfig) {
        String configuredTiers = tableConfig.getTableCustom(TIER_NAMES_SUFFIX);
        log.trace("Tier names using property {} for Table id: {} are {}", Property.TABLE_ARBITRARY_PROP_PREFIX + TIER_NAMES_SUFFIX, tableId, configuredTiers);
        if (configuredTiers == null) {
            log.warn("Table property {} is not set, must configure tiers.", Property.TABLE_ARBITRARY_PROP_PREFIX + TIER_NAMES_SUFFIX);
            return Set.of();
        }
        return Arrays.stream(configuredTiers.split(",", -1)).map(String::trim).filter(tier -> !tier.isEmpty())
                        .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    public static String getTierProperty(TableId tableId, ServiceEnvironment.Configuration tableConfig, String tier, String suffix) {
        String value = tableConfig.getTableCustom(PROPERTY_PREFIX + tier + suffix);
        log.trace("Property {} for Table id: {} is {}", Property.TABLE_ARBITRARY_PROP_PREFIX + PROPERTY_PREFIX + tier + suffix, tableId, value);
        return value;
    }

    public static long getTierDaysBack(TableId tableId, ServiceEnvironment.Configuration tableConfig, String tier) {
        return Long.parseLong(getTierProperty(tableId, tableConfig, tier, DAYS_BACK_SUFFIX));
    }
}

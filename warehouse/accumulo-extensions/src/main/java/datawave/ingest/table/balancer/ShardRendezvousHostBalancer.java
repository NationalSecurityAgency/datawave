package datawave.ingest.table.balancer;

import static java.time.temporal.ChronoUnit.DAYS;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.TabletId;
import org.apache.accumulo.core.spi.balancer.BalancerEnvironment;
import org.apache.accumulo.core.spi.balancer.data.TabletServerId;
import org.apache.accumulo.core.spi.common.ServiceEnvironment.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.ingest.table.volumeChoosers.ShardedTableDateBasedTieredVolumeChooser;

/**
 * An alternative to the #ShardedTableTabletBalancer for balancing the shard table. There are four key differences with this balancer. First, it balances each
 * day independently using rendezvous hashing, the advantage of this is that adding/removing/changing one day does not impact the balancing decisions for all
 * other days. Second, it evens the shards in a day out across host and then tablet servers on a host. Third, it makes the same decisions when computing
 * assignments and balancing, the #ShardedTableTabletBalancer can make different decisions in computing assignments that balance computation will undo. Fourth,
 * it allows partitioning the tablets servers and tablets by time, for example can assign tablets that are from 0 to 20 days old to tablet server group1
 * (defined by a regex) and tablets older than 20 days to tablet server group2.
 *
 * <p>
 * Since this balancer uses rendezvous hashing it will not achieve exactly num_tablets/num_tservers tablet per tserver. In testing 70% of tservers had a tablet
 * count within one standard deviation of num_tablets/num_tservers. 95% of tservers had a tablet count within two standard deviations of
 * num_tablets/num_tservers.
 * </p>
 *
 * <p>
 * Configuration of this balancer builds on the configuration of the {@link ShardedTableDateBasedTieredVolumeChooser}
 * </p>
 * balancer by interleaving tserver properties within its properties. Below is an example of configuring this balancer to use tablet severs that match the regex
 * {@code host0000.*} for shards 0 to 20 days old. Tablet servers that match the regex {@code host000[1-9].*} are used for shards over 20 days old. The host
 * selected by the regex for each tier do not have to be disjoint. Tablets within each tier will only use the host that match the regex, but it does not care if
 * another tier uses those hosts.
 *
 * <pre>
 *         table.custom.volume.tier.names=t1,t2
 *         table.custom.volume.tiered.t1.days.back=0
 *         table.custom.volume.tiered.t1.tservers=host0000.*
 *         table.custom.volume.tiered.t2.days.back=20
 *         table.custom.volume.tiered.t2.tservers=host000[1-9].*
 *         table.custom.sharded.balancer.max.migrations=1000
 * </pre>
 */
public class ShardRendezvousHostBalancer extends RendezvousHostBalancer {

    private static final Logger log = LoggerFactory.getLogger(ShardRendezvousHostBalancer.class);

    static final ShardedTableTabletBalancer.ShardDayPartitioner PARTITIONER = new ShardedTableTabletBalancer.ShardDayPartitioner();
    private static final String TSERVERS_PREFIX = ".tservers";
    private static final String SHARDED_PROPERTY_PREFIX = Property.TABLE_ARBITRARY_PROP_PREFIX.getKey() + "sharded.balancer.";
    public static final String SHARDED_MAX_MIGRATIONS = SHARDED_PROPERTY_PREFIX + "max.migrations";
    public static final int MAX_MIGRATIONS_DEFAULT = 10000;

    private Configuration tableConfig;

    public ShardRendezvousHostBalancer(TableId tableId) {
        super(tableId);
    }

    @Override
    public void init(BalancerEnvironment balancerEnvironment) {
        super.init(balancerEnvironment);
        this.tableConfig = balancerEnvironment.getConfiguration(tableIdToBalance);
    }

    @Override
    protected String getTabletGroup(TabletId tabletId) {
        return PARTITIONER.apply(tabletId);
    }

    @Override
    protected int getMaxMigrations() {
        return getMaxMigrations(() -> tableConfig);
    }

    public static int getMaxMigrations(Supplier<Configuration> tableConfig) {
        int maxMigrations = MAX_MIGRATIONS_DEFAULT;
        try {
            String maxMigrationsProp = tableConfig.get().get(SHARDED_MAX_MIGRATIONS);
            if (maxMigrationsProp != null && !maxMigrationsProp.isEmpty()) {
                try {
                    maxMigrations = Integer.parseInt(maxMigrationsProp);
                } catch (Exception e) {
                    log.error("Unable to parse {} value ({}}) as an integer.  Defaulting to {}", SHARDED_MAX_MIGRATIONS, maxMigrationsProp, maxMigrations);
                }
            }
        } catch (Exception e) {
            log.warn("Failed to get {}}.  Defaulting to {}", SHARDED_MAX_MIGRATIONS, maxMigrations, e);
        }
        return maxMigrations;
    }

    private List<Map.Entry<Long,Matcher>> getTiers(Configuration tableConfig) {
        List<Map.Entry<Long,Matcher>> configuredTiers = new ArrayList<>();
        Set<String> tiers = ShardedTableDateBasedTieredVolumeChooser.listTiers(tableIdToBalance, tableConfig);
        for (String tier : tiers) {
            long daysBack = ShardedTableDateBasedTieredVolumeChooser.getTierDaysBack(tableIdToBalance, tableConfig, tier);
            String regex = ShardedTableDateBasedTieredVolumeChooser.getTierProperty(tableIdToBalance, tableConfig, tier, TSERVERS_PREFIX);
            var pattern = Pattern.compile(regex);
            configuredTiers.add(new AbstractMap.SimpleImmutableEntry<>(daysBack, pattern.matcher("")));
        }
        return configuredTiers;
    }

    protected List<Long> getDaysBack(List<Map.Entry<Long,Matcher>> configuredTiers, TabletServerId tabletServerId) {
        ArrayList<Long> db = new ArrayList<>();
        for (var entry : configuredTiers) {
            long daysBack = entry.getKey();
            Matcher matcher = entry.getValue();
            matcher.reset(tabletServerId.getHost() + ":" + tabletServerId.getPort());
            if (matcher.matches()) {
                db.add(daysBack);
            }
        }
        return db;
    }

    protected LocalDate today() {
        return LocalDate.now();
    }

    public static LocalDate parse(String date) {
        try {
            return LocalDate.parse(date, FORMATTER);
        } catch (DateTimeParseException e) {
            log.warn("Shard: {} is not formatted correctly", date);
            return LocalDate.EPOCH;
        }

    }

    private static final String DATE_PATTERN = "yyyyMMdd";
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern(DATE_PATTERN);

    @Override
    protected Function<String,List<TabletServerId>> getServerPartitioner(Collection<TabletServerId> allTservers) {

        final List<Map.Entry<Long,Matcher>> configuredTiers = getTiers(tableConfig);

        if (configuredTiers.isEmpty()) {
            throw new IllegalStateException("Tier configuration is not set");
        }

        TreeMap<Long,List<TabletServerId>> serverPartitioningMap = new TreeMap<>();

        allTservers.forEach(tabletServerId -> {
            var daysBack = getDaysBack(configuredTiers, tabletServerId);
            if (!daysBack.isEmpty()) {
                for (var db : daysBack) {
                    log.trace(" daysBack:{} tserver:{}", db, tabletServerId);
                    serverPartitioningMap.computeIfAbsent(db, ub -> new ArrayList<>()).add(tabletServerId);
                }
            } else {
                log.warn("Tserver {} did not match any tiers", tabletServerId);
            }
        });

        // grab this once outside the lambda so the lambda always makes consistent decisions.
        var today = today();

        return tabletGroup -> {
            LocalDate rowDate = tabletGroup.equals("null") ? today : parse(tabletGroup);
            long days = DAYS.between(rowDate, today);
            var entry = serverPartitioningMap.floorEntry(days);
            if (entry == null) {
                if (days < 0) {
                    // This date is in the future
                    entry = serverPartitioningMap.firstEntry();
                } else {
                    entry = serverPartitioningMap.lastEntry();
                }
            }
            return entry == null ? List.of() : entry.getValue();
        };
    }
}

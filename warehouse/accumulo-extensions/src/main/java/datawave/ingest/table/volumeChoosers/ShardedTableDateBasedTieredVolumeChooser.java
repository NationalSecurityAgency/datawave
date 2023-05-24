package datawave.ingest.table.volumeChoosers;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.spi.common.ServiceEnvironment;
import org.apache.accumulo.core.spi.fs.RandomVolumeChooser;
import org.apache.accumulo.core.spi.fs.VolumeChooserEnvironment;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.time.temporal.ChronoUnit.DAYS;

/**
 * This class is used to configure a date based volume chooser for sharded tables. To configure, this relies on a few accumulo properties being set on the
 * table. Those properties are:
 *
 * 1.{@link Property#TABLE_ARBITRARY_PROP_PREFIX}.{@value TIER_NAMES_SUFFIX} 2. Some number of properties following the pattern
 * {@link Property#TABLE_ARBITRARY_PROP_PREFIX}{@value PROPERTY_PREFIX}.&lt;tierName&gt;.{@value VOLUME_SUFFIX} 3. The same number of properties following the
 * pattern {@link Property#TABLE_ARBITRARY_PROP_PREFIX}{@value PROPERTY_PREFIX}.&lt;tierName&gt;.{@value DAYS_BACK_SUFFIX}
 *
 * The volume chooser will compute the number of days back for the current endRow and choose from the volumes with the next highest daysBack setting.
 *
 * EG: Properties Set: 1. {@link Property#TABLE_ARBITRARY_PROP_PREFIX}.{@value TIER_NAMES_SUFFIX} = new,old 2.
 * {@link Property#TABLE_ARBITRARY_PROP_PREFIX}{@value PROPERTY_PREFIX}.new.{@value VOLUME_SUFFIX} = newData 2.
 * {@link Property#TABLE_ARBITRARY_PROP_PREFIX}{@value PROPERTY_PREFIX}.new.{@value DAYS_BACK_SUFFIX} = 0 3.
 * {@link Property#TABLE_ARBITRARY_PROP_PREFIX}{@value PROPERTY_PREFIX}.old.{@value VOLUME_SUFFIX} = oldData 4.
 * {@link Property#TABLE_ARBITRARY_PROP_PREFIX}{@value PROPERTY_PREFIX}.old.{@value DAYS_BACK_SUFFIX} = 125
 *
 * Data dated in the future will be treated as newData Data that is 124 days old or newer will be written to newData Data that is 125 days or older will be
 * written to oldData
 *
 */
public class ShardedTableDateBasedTieredVolumeChooser extends RandomVolumeChooser {
    private static final Logger log = LoggerFactory.getLogger(ShardedTableDateBasedTieredVolumeChooser.class);
    private static final String TIER_NAMES_SUFFIX = "volume.tier.names";
    private static final String PROPERTY_PREFIX = "volume.tiered.";
    private static final String VOLUME_SUFFIX = ".volumes";
    private static final String DAYS_BACK_SUFFIX = ".days.back";
    private static final String DATE_PATTERN = "yyyyMMdd";
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern(DATE_PATTERN);
    
    private static Pattern SHARD_PATTERN = Pattern.compile("\\d{8}_\\d+");
    
    @Override
    public String choose(VolumeChooserEnvironment env, Set<String> options) {
        
        if (!env.getTable().isPresent() || !env.getChooserScope().equals(VolumeChooserEnvironment.Scope.TABLE))
            return super.choose(env, options);
        else {
            TableId tableId = env.getTable().get();
            ServiceEnvironment.Configuration tableConfig = env.getServiceEnv().getConfiguration(tableId);
            // Get variables
            log.trace("Determining tier names using property {} for Table id: {}", Property.TABLE_ARBITRARY_PROP_PREFIX + TIER_NAMES_SUFFIX, tableId);
            String configuredTiers = tableConfig.getTableCustom(TIER_NAMES_SUFFIX);
            TreeMap<Long,Set<String>> daysToVolumes = getTiers(tableId, tableConfig, options, configuredTiers);
            
            Text endRow = env.getEndRow();
            
            Long floorKey = daysToVolumes.ceilingKey(0L);
            if (endRow == null) {
                // this is the default tablet. No shard means this is new data
                options = floorKey == null ? options : daysToVolumes.get(floorKey);
                
            } else {
                String endRowString = endRow.toString();
                if (SHARD_PATTERN.matcher(endRowString).matches()) {
                    String date = endRowString.substring(0, 8);
                    LocalDate rowDate = LocalDate.parse(date, FORMATTER);
                    LocalDate today = LocalDate.now();
                    long days = DAYS.between(rowDate, today);
                    floorKey = daysToVolumes.floorKey(days > 0L ? days : 0L);
                    options = floorKey == null ? options : daysToVolumes.get(floorKey);
                } else {
                    log.warn("endRow does not match pattern. Is this table sharded? endRow was " + endRowString + " and tableId is " + tableId);
                }
            }
            return super.choose(env, options);
        }
    }
    
    private TreeMap<Long,Set<String>> getTiers(TableId tableId, ServiceEnvironment.Configuration tableConfig, Set<String> options, String configuredTiers) {
        TreeMap<Long,Set<String>> daysToVolumes = new TreeMap<>();
        daysToVolumes.put(Long.MAX_VALUE, options);
        for (String tier : StringUtils.split(configuredTiers, ',')) {
            log.debug("Determining volumes for tier {} using property {} for Table id: {}", tier,
                            Property.TABLE_ARBITRARY_PROP_PREFIX + PROPERTY_PREFIX + tier + VOLUME_SUFFIX, tableId);
            Set<String> volumesForCurrentTier = Arrays.stream(StringUtils.split(tableConfig.getTableCustom(PROPERTY_PREFIX + tier + VOLUME_SUFFIX), ','))
                            .collect(Collectors.toSet());
            long daysBackForCurrentTier = Long.parseLong(tableConfig.getTableCustom(PROPERTY_PREFIX + tier + DAYS_BACK_SUFFIX));
            if (daysBackForCurrentTier >= 0) {
                if (volumesForCurrentTier.size() < 1) {
                    throw new IllegalStateException("Volumes list empty for tier " + tier + ". Ensure property " + Property.TABLE_ARBITRARY_PROP_PREFIX
                                    + PROPERTY_PREFIX + tier + VOLUME_SUFFIX + " is set");
                }
                daysToVolumes.put(daysBackForCurrentTier, volumesForCurrentTier);
            } else
                throw new IllegalStateException("Invalid days back for " + tier + ". Must be >= 0");
        }
        return daysToVolumes;
    }
    
}

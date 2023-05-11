package datawave.ingest.table.volumeChoosers;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.spi.common.ServiceEnvironment;
import org.apache.accumulo.core.spi.fs.RandomVolumeChooser;
import org.apache.accumulo.core.spi.fs.VolumeChooserEnvironment;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static java.time.temporal.ChronoUnit.DAYS;

public class ShardedTableDateBasedTieredVolumeChooser extends RandomVolumeChooser {
    private static final Logger log = LoggerFactory.getLogger(ShardedTableDateBasedTieredVolumeChooser.class);
    private static final String TIER_NAMES_SUFFIX = "volume.tier.names";
    private final String PROPERTY_PREFIX = "volume.tiered.";
    private final String VOLUME_SUFFIX = ".volumes";
    private static final String DAYS_BACK_SUFFIX = ".days.back";
    private static final String DATE_PATTERN = "yyyyMMdd";
    
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
            TreeMap<Long, Set<String>> daysToVolumes = getTiers(tableId, tableConfig, options, configuredTiers);

            String endRow = env.getEndRow().toString();

            if (endRow.matches("\\d{8}_\\d+")) {
                String date = endRow.substring(0, 8);
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern(DATE_PATTERN);
                LocalDate rowDate = LocalDate.parse(date, formatter);
                LocalDate today = LocalDate.now();
                long days = DAYS.between(rowDate, today);
                Long ceilingKey = daysToVolumes.ceilingKey(days);
                options = ceilingKey == null ? options : daysToVolumes.get(ceilingKey);

            }
            return super.choose(env, options);
        }
    }
    
    private TreeMap<Long,Set<String>> getTiers(TableId tableId, ServiceEnvironment.Configuration tableConfig, Set<String> options, String configuredTiers) {
        TreeMap<Long,Set<String>> daysToVolumes = new TreeMap<>();
        daysToVolumes.put(Long.MAX_VALUE, options);
        for (String tier : StringUtils.split(configuredTiers, ',')) {
            log.debug("Determining volumes for tier {} using property {} for Table id: {}", tier, Property.TABLE_ARBITRARY_PROP_PREFIX + PROPERTY_PREFIX + tier
                            + VOLUME_SUFFIX, tableId);
            Set<String> volumesForCurrentTier = Arrays.stream(StringUtils.split(
                    tableConfig.getTableCustom(PROPERTY_PREFIX + tier + VOLUME_SUFFIX), ',')).collect(Collectors.toSet());
            long daysBackForCurrentTier = Long.parseLong(tableConfig
                            .getTableCustom(PROPERTY_PREFIX + tier + DAYS_BACK_SUFFIX));
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

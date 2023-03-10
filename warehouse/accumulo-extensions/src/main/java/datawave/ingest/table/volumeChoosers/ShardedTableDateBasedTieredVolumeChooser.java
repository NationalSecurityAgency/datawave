package datawave.ingest.table.volumeChoosers;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.server.fs.RandomVolumeChooser;
import org.apache.accumulo.server.fs.VolumeChooserEnvironment;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.TreeMap;

import static java.time.temporal.ChronoUnit.DAYS;

public class ShardedTableDateBasedTieredVolumeChooser extends RandomVolumeChooser {
    private static final Logger log = LoggerFactory.getLogger(ShardedTableDateBasedTieredVolumeChooser.class);
    private static final String TIER_NAMES_SUFFIX = "volume.tier.names";
    private final String PROPERTY_PREFIX = "volume.tiered.";
    private final String VOLUME_SUFFIX = ".volumes";
    private static final String DAYS_BACK_SUFFIX = ".days.back";
    private static final String DATE_PATTERN = "yyyyMMdd";
    
    @Override
    public String choose(VolumeChooserEnvironment env, String[] options) {
        
        if (!env.hasTableId() || !env.getScope().equals(VolumeChooserEnvironment.ChooserScope.TABLE))
            return super.choose(env, options);
        
        // Get variables
        log.trace("Determining tier names using property {} for Table id: {}", Property.TABLE_ARBITRARY_PROP_PREFIX + TIER_NAMES_SUFFIX, env.getTableId());
        String configuredTiers = env.getServiceEnv().getConfiguration(env.getTableId()).getTableCustom(TIER_NAMES_SUFFIX);
        TreeMap<Long,String[]> daysToVolumes = getTiers(env, options, configuredTiers);
        
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
    
    private TreeMap<Long,String[]> getTiers(VolumeChooserEnvironment env, String[] options, String configuredTiers) {
        TreeMap<Long,String[]> daysToVolumes = new TreeMap<>();
        daysToVolumes.put(Long.MAX_VALUE, options);
        for (String tier : StringUtils.split(configuredTiers, ',')) {
            log.debug("Determining volumes for tier {} using property {} for Table id: {}", tier, Property.TABLE_ARBITRARY_PROP_PREFIX + PROPERTY_PREFIX + tier
                            + VOLUME_SUFFIX, env.getTableId());
            String[] volumesForCurrentTier = StringUtils.split(
                            env.getServiceEnv().getConfiguration(env.getTableId()).getTableCustom(PROPERTY_PREFIX + tier + VOLUME_SUFFIX), ',');
            long daysBackForCurrentTier = Long.parseLong(env.getServiceEnv().getConfiguration(env.getTableId())
                            .getTableCustom(PROPERTY_PREFIX + tier + DAYS_BACK_SUFFIX));
            if (daysBackForCurrentTier >= 0) {
                if (volumesForCurrentTier.length < 1) {
                    throw new VolumeChooserException("Volumes list empty for tier " + tier + ". Ensure property " + Property.TABLE_ARBITRARY_PROP_PREFIX
                                    + PROPERTY_PREFIX + tier + VOLUME_SUFFIX + " is set");
                }
                daysToVolumes.put(daysBackForCurrentTier, volumesForCurrentTier);
            } else
                throw new VolumeChooserException("Invalid days back for " + tier + ". Must be >= 0");
        }
        return daysToVolumes;
    }
    
}

package datawave.volumeChoosers;

import com.google.common.base.Preconditions;
import org.apache.accumulo.server.fs.RandomVolumeChooser;
import org.apache.accumulo.server.fs.VolumeChooserEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class ShardedTableDateBasedTieredVolumeChooser  extends RandomVolumeChooser {
     private static final Logger log = LoggerFactory.getLogger(ShardedTableDateBasedTieredVolumeChooser.class);
     private static final String NEW_SUFFIX = "volume.dates.tiered.new";
     private static final String OLD_SUFFIX = "volume.dates.tiered.old";
     private static final String DAYS_BACK_SUFFIX = "volume.dates.tiered.days.back";
     private static final String DATE_PATTERN = "yyyyMMdd";

     @Override
     public String choose(VolumeChooserEnvironment env, String[] options) {

          if (!env.hasTableId() || !env.getScope().equals(VolumeChooserEnvironment.ChooserScope.TABLE))
               return super.choose(env, options);

          // Get variables
          log.trace("Looking up property {} + for Table id: {}", NEW_SUFFIX, env.getTableId());
          String[] preferredVolumesForRecentData = env.getServiceEnv().getConfiguration(env.getTableId()).getTableCustom(NEW_SUFFIX).split(",");
          String[] preferredVolumesForOlderData = env.getServiceEnv().getConfiguration(env.getTableId()).getTableCustom(OLD_SUFFIX).split(",");

          int numberOfDaysBack;
          try {
               numberOfDaysBack = Integer.parseInt(env.getServiceEnv().getConfiguration(env.getTableId()).getTableCustom(DAYS_BACK_SUFFIX));
          } catch (NumberFormatException e)
          {
               throw new VolumeChooserException("Unable to parse days back as integer", e);
          }
          if(numberOfDaysBack<0)
          {
               throw new VolumeChooserException("Negative days back not supported. Please set a positive value.");
          }
          String endRow = env.getEndRow().toString();

          if (endRow.matches("\\d{8}_\\d+")) {
               String date = endRow.substring(0, 8);
               DateTimeFormatter formatter = DateTimeFormatter.ofPattern(DATE_PATTERN);
               LocalDate rowDate = LocalDate.parse(date, formatter);
               LocalDate today = LocalDate.now();

               Set<String> optionsSet = new HashSet<String>(Arrays.asList(options));
               Set<String> tieredOptions;
               if (rowDate.isBefore(today.minusDays(numberOfDaysBack))) {
                    tieredOptions = new HashSet<String>(Arrays.asList(preferredVolumesForOlderData));
               } else {
                    tieredOptions = new HashSet<String>(Arrays.asList(preferredVolumesForRecentData));
               }
               optionsSet.retainAll(tieredOptions);
               options = optionsSet.toArray(new String[optionsSet.size()]);
               if (options.length < 1) {
                    throw new VolumeChooserException("Unable to make choice as the intersection of preferred " +
                            "volumes for this date range is empty. " +
                            "Endrow: " + endRow + " Date: " + rowDate);
               }
          }

          return super.choose(env, options);
     }
    
}

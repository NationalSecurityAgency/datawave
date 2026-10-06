package datawave.ingest.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.apache.accumulo.core.client.admin.TabletAvailability;
import org.apache.accumulo.core.client.admin.TabletInformation;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.jupiter.api.Test;

class GenerateShardSplitsTest {

    @Test
    void sortSplitsByMidpoints() {
        List<Text> files = List.of(new Text("20230101_1"), new Text("20230101_2"), new Text("20230101_3"), new Text("20230102_1"), new Text("20230102_2"),
                        new Text("20230102_3"), new Text("20230103_1"), new Text("20230103_2"), new Text("20230103_3"), new Text("20230103_10"));

        List<Text> expected = List.of(new Text("20230102_2"), new Text("20230102_3"), new Text("20230101_2"), new Text("20230101_3"), new Text("20230101_1"),
                        new Text("20230102_1"), new Text("20230103_2"), new Text("20230103_3"), new Text("20230103_1"), new Text("20230103_10"));
        List<Text> results = new ArrayList<>(files.size());
        GenerateShardSplits.calculateMidpoints(files, results);

        assertEquals(expected, results, "Sort order was incorrect");

        for (Text element : files) {
            Assert.assertTrue(expected.contains(element));
        }

    }

    private static TabletInformation tablet(TabletAvailability availability, String location) {
        TabletInformation tablet = mock(TabletInformation.class);
        when(tablet.getTabletAvailability()).thenReturn(availability);
        when(tablet.getLocation()).thenReturn(Optional.ofNullable(location));
        return tablet;
    }

    @Test
    void onlyHostedTabletsAreWaitedOn() {
        assertFalse(GenerateShardSplits.anyHosted(List.of(tablet(TabletAvailability.ONDEMAND, null), tablet(TabletAvailability.UNHOSTED, null))));
        assertTrue(GenerateShardSplits.anyHosted(List.of(tablet(TabletAvailability.ONDEMAND, null), tablet(TabletAvailability.HOSTED, null))));
        assertFalse(GenerateShardSplits.anyHosted(List.of()));
    }

    @Test
    void onlyCurrentLocationsAreCounted() {
        List<TabletInformation> tablets = List.of(tablet(TabletAvailability.HOSTED, "CURRENT:host1:9997"),
                        tablet(TabletAvailability.HOSTED, "CURRENT:host1:9997"), tablet(TabletAvailability.HOSTED, "CURRENT:host2:9997"),
                        tablet(TabletAvailability.HOSTED, "FUTURE:host3:9997"), tablet(TabletAvailability.HOSTED, null));
        assertEquals(Set.of("host1:9997", "host2:9997"), GenerateShardSplits.currentLocations(tablets));
    }
}

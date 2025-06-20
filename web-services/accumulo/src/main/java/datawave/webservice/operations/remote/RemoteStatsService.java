package datawave.webservice.operations.remote;

import javax.annotation.PostConstruct;

import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.databind.ObjectReader;

import datawave.webservice.response.StatsResponse;

public class RemoteStatsService extends RemoteAccumuloService {

    private static final String STATS_SUFFIX = "stats";

    private ObjectReader statsReader;

    @Override
    @PostConstruct
    public void init() {
        super.init();
        statsReader = objectMapper.readerFor(StatsResponse.class);
    }

    @Timed(name = "dw.remoteAccumuloService.stats", absolute = true)
    public StatsResponse getStats() {
        return execGet(STATS_SUFFIX, statsReader);
    }
}

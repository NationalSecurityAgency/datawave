package datawave.webservice.query.runner;

import datawave.core.query.cache.ResultsPage;
import datawave.microservice.querymetric.BaseQueryMetric.Lifecycle;

enum PageStopReason {
    EXHAUSTED(ResultsPage.Status.COMPLETE, true, null),
    RESULT_LIMIT(ResultsPage.Status.COMPLETE, true, Lifecycle.MAXRESULTS),
    WORK_LIMIT(ResultsPage.Status.COMPLETE, true, Lifecycle.MAXWORK),
    FULL_PAGE(ResultsPage.Status.COMPLETE, false, null),
    BYTE_LIMIT(ResultsPage.Status.PARTIAL, false, null),
    DEADLINE(ResultsPage.Status.PARTIAL, false, null),
    FORCED(ResultsPage.Status.PARTIAL, false, null),
    INTERMEDIATE(ResultsPage.Status.PARTIAL, false, null);

    final ResultsPage.Status status;
    final boolean terminal;
    final Lifecycle lifecycle;

    PageStopReason(ResultsPage.Status status, boolean terminal, Lifecycle lifecycle) {
        this.status = status;
        this.terminal = terminal;
        this.lifecycle = lifecycle;
    }
}

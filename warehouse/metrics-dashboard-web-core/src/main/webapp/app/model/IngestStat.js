Ext.define('Datawave.model.IngestStat', {
    extend: 'Ext.data.Model',
    idField: 'dateTime',
    fields: [
        {name: 'dateTime', dateFormat: 'u'},
        'liveEventCount', 'liveAveTime', 'liveEventRate', 'liveAvePollerTime', 'liveAveIngestWaitTime', 'liveAveIngestTime',
        'bulkEventCount', 'bulkAveTime', 'bulkEventRate', 'bulkAvePollerTime', 'bulkAveIngestWaitTime', 'bulkAveIngestTime', 'bulkAveLoaderWaitTime', 'bulkAveLoaderTime'
    ]
});

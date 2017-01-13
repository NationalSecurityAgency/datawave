/**
 * Global application settings, overrides, and configurations.
 */
Ext.Loader.setConfig({
    enabled: true
});

Ext.define('Datawave.Constants', {
    singleton: true,
    deployed: String(document.location).indexOf('file:///') === -1,
    /**
    sanitize: Ext.Object.fromQueryString(document.location.search).sanitize,
	*/
    sanitize: true,
    
    dateFormat: 'Y/m/d H:i',
    ingestURL: '../DataWave/Ingest/Metrics/services/dashboard',
    queryURL: '../DataWave/Query/Metrics/dashboard/stats.json',
    opacity: 0.8,
    
    queryCountTitles: ['< 3 Sec', '< 10 Sec', '< 60 Sec', '>= 60 Sec', 'Errors'],
    queryCountFields: ['upTo3Sec', 'upTo10Sec', 'upTo60Sec', 'moreThan60Sec', 'errorCount'],
    
    resultCountTitles: ['< 10K', '< 1M', '>= 1M', '0', 'Errors'],
    resultCountFields: ['upTo10KResults', 'upTo1MResults', 'upToINFResults', 'zeroResults', 'errorCount'],
    
    selectorCountTitles: ['1', '< 16', '< 100', '< 1000', '>= 1000'],
    selectorCountFields: ['oneTerm', 'upTo16Terms', 'upTo100Terms', 'upTo1000Terms', 'upToInfTerms'],

    ingestIntervalMinutes: 15,
    queryIntervalMinutes: 60
});

Ext.override(Ext.data.Connection, {
    withCredentials: true, //make sure we always include the client certificate
    method: 'GET'//default ajax requests to GET
});

Ext.Ajax.timeout = 10 * 60 * 1000; // change from 30s to 10 minutes
Ext.override(Ext.data.proxy.Server, {timeout: Ext.Ajax.timeout});
Ext.override(Ext.data.Connection, {timeout: Ext.Ajax.timeout});

Ext.application({
    name: 'Datawave',
    autoCreateViewport: true,
    controllers: (Datawave.Constants.deployed === true) ? ['MainController', 'IngestController', 'TitleController', 'RealTimeController'] : [],
    models: [
        'IngestStat',
        'QueryStat',
        'SummaryStat'
    ],
    stores: [
        'IngestStats',
        'QueryStats'
    ],
    views: [
        'ingest.IngestBulkEventChart',
        'ingest.IngestLiveEventChart',
        'ingest.IngestBulkLatencyChart',
        'ingest.IngestLiveLatencyChart',
        'ingest.IngestPanel',
        'query.QueryCountChart',
        'query.ResultCountChart',
        'query.SelectorCountChart',
        'query.GaugePanel',
        'query.QueryPanel'
    ],
    init: function() {
        Ext.Ajax.on('requestexception', function(conn, response, options) {
            Ext.Msg.show({
                msg: 'Oops. I am unable to fullfil your request. Please contact an administrator.',
                width: 300,
                buttons: Ext.Msg.OK,
                title: 'Server error'
            });
        });
    }
});

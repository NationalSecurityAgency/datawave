Ext.define('Datawave.view.ingest.IngestPanel', {
    extend: 'Ext.panel.Panel',
    alias: 'widget.ingestpanel',
    layout: {
        type: 'accordion',
        multi: true
    },
    border: false,
    defaultType: 'panel',
    defaults: {
        border: false,
        layout: 'fit'
    },
    items: [
        {
            title: 'Live Ingest Events',
            items: [
                {
                    xtype: 'ingestliveeventchart'
                }
            ],
            padding: '0 0 5 0'
        },
        {
            title: 'Bulk Ingest Events',
            items: [
                {
                    xtype: 'ingestbulkeventchart'
                }
            ]
        },
        {
            title: 'Average Live Ingest Latency (seconds)',
            items: [
                {
                    xtype: 'ingestlivelatencychart'
                }
            ]
        },
        {
            title: 'Average Bulk Ingest Latency (seconds)',
            items: [
                {
                    xtype: 'ingestbulklatencychart'
                }
            ]
        }
    ]
});

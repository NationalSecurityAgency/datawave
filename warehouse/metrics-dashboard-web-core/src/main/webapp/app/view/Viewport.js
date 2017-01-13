Ext.define('Datawave.view.Viewport', {
    extend: 'Ext.container.Viewport',
    layout: {
        type: 'border'
    },
    border: false,
    padding: '0',
    defaults: {
        border: false
    },
    items: [
        {
            xtype: 'tabpanel',
            region: 'center',
            tabPosition: 'bottom',
            items: [
                {
                    title: 'Query',
                    xtype: 'querypanel'
                },
                {
                    title: 'Ingest',
                    xtype: 'ingestpanel'
                }
            ]
        }
    ]
});

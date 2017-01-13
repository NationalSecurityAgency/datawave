var store = Ext.create('Ext.data.Store', {
    storeId: 'realTimeStore',
    model: 'Datawave.model.QueryStat',
    autoLoad: false,
    proxy: {
        url: Datawave.Constants.queryURL,
        type: 'ajax',
        reader: {
            type: 'json',
            root: 'data'
        }
    }
});

var chartConfig = function(field) {
    return Ext.create('Ext.chart.Chart', {
        store: store,
        animate: true,
        shadow: true,
        style: 'background-color: #fff',
        insetPadding: 25,
        axes: [
            {
                type: 'Numeric',
                position: 'left',
                fields: [field],
                grid: true,
                minimum: 0
            },
            {
                type: 'Time',
                position: 'bottom',
                fields: ['dateTime'],
                dateFormat: 'Y/m/d H:i',
//                label: {
//                    renderer: function(v) {
//                        var date = new Date(v),
//                                minutes = date.getMinutes();
//                        
//                        if (minutes % 15 === 0) {
//                            return Ext.util.Format.date(date, 'H:i');
//                        }
//                        
//                        return '';
//                    }
//                },
                grid: false,
                step: [Ext.Date.MINUTE, 15]
            }
        ],
        series: [
            {
                type: 'line',
                axis: 'left',
                fill: true,
                highlight: true,
                xField: 'dateTime',
                yField: field,
                smooth: false
            }
        ]
    });
};

Ext.define('Datawave.view.query.GaugePanel', {
    extend: 'Ext.panel.Panel',
    alias: 'widget.gaugepanel',
    border: false,
    defaultType: 'panel',
    defaults: {
        border: true,
        layout: 'fit',
        flex: 1
    },
    items: [
        {items: [chartConfig('queryCount')]},
        {items: [chartConfig('resultCount')]},
        {items: [chartConfig('selectorCount')]}
    ]
});

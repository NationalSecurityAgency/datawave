var countBarConfig = function(storeId) {
    return Ext.create('Ext.chart.Chart', {
        store: Ext.create('Ext.data.ArrayStore', {
            model: 'Datawave.model.SummaryStat',
            storeId: storeId
        }),
        animate: true,
        style: 'background-color: #fff',
        insetPadding: 25,
        axes: [
            {
                type: 'numeric',
                position: 'bottom',
                fields: ['data'],
                label: {
                    renderer: Ext.util.Format.numberRenderer('0,0')
                },
                grid: true,
                minimum: 0
            },
            {
                type: 'Category',
                position: 'left',
                fields: 'name'
            }
        ],
        series: [
            {
                type: 'bar',
                axis: 'bottom',
                xField: 'name',
                yField: 'data',
                highlight: true,
                label: {
                    display: 'insideEnd',
                    field: 'data',
                    renderer: Ext.util.Format.numberRenderer('0,0'),
                    orientation: 'horizontal',
                    color: '#333',
                    'text-anchor': 'middle'
                }
            }
        ]
    });
};

var countChartConfig = function(titles, fields) {
    return Ext.create('Ext.chart.Chart', {
        store: 'QueryStats',
        alias: 'widget.querycountchart',
        animate: true,
        shadow: true,
        style: 'background-color: #fff',
        legend: true,
        axes: [
            {
                type: 'Numeric',
                position: 'left',
                fields: fields,
                grid: true,
                label: {
                    renderer: Ext.util.Format.numberRenderer('0,0')
                },
                minimum: 0
            },
            {
                type: 'Time',
                position: 'bottom',
                fields: ['dateTime'],
                label: {
                    renderer: function(v) {
                        var date = new Date(v),
                                hours = date.getHours();

                        if (hours === 0) {
                            return Ext.util.Format.date(date, 'Y/m/d');
                        }
                        else if (hours === 6) {
                            return '0600';
                        }
                        else if (hours === 12) {
                            return '1200';
                        }
                        else if (hours === 18) {
                            return '1800';
                        }
                        else {
                            return '';
                        }
                    }
                },
                grid: false,
                step: [Ext.Date.MINUTE, Datawave.Constants.queryIntervalMinutes]
            }
        ],
        series: [
            {
                type: 'column',
                xField: 'dateTime',
                yField: fields,
                title: titles,
                stacked: true,
                style: {
                    opacity: Datawave.Constants.opacity
                }
            }
        ]
    });
};

var rowConfig = function(title1, title2, xtype2, storeId) {
    return {
        layout: {
            type: 'hbox',
            align: 'stretch'
        },
        items: [
            {
                title: title1,
                xtype: 'panel',
                flex: 1,
                layout: 'fit',
                items: [
                    countBarConfig(storeId)
                ]
            },
            {
                title: title2,
                xtype: 'panel',
                flex: 5,
                layout: 'fit',
                items: [
                    {
                        xtype: xtype2
                    }
                ]
            }
        ],
        flex: 2
    };
};

Ext.define('Datawave.view.query.QueryPanel', {
    extend: 'Ext.panel.Panel',
    alias: 'widget.querypanel',
    layout: {
        type: 'vbox',
        align: 'stretch'
    },
    border: false,
    defaultType: 'panel',
    defaults: {
        border: false,
        layout: 'fit'
    },
    items: [
        {
            title: 'Query Latency Percentages (updated every 30 seconds)',
            xtype: 'gaugepanel',
            flex: 1,
            layout: {
                type: 'hbox',
                align: 'stretch'
            }
        },
        rowConfig('Query Latency Overview', 'Query Latency Groups by Hour', 'querycountchart', 'queryBarStore'),
        rowConfig('Resultset Size Overview', 'Resultset Size Groups by Hour', 'resultcountchart', 'resultBarStore'),
        rowConfig('Query Term Counts Overview', 'Query Term Counts by Hour', 'selectorcountchart', 'selectorBarStore')
    ]
});

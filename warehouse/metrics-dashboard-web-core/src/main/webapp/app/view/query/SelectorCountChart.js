Ext.define('Datawave.view.query.SelectorCountChart', {
    extend: 'Ext.chart.Chart',
    store: 'QueryStats',
    alias: 'widget.selectorcountchart',
    animate: true,
    shadow: true,
    style: 'background-color: #fff',
    legend: true,
    axes: [
        {
            type: 'Numeric',
            position: 'left',
            fields: Datawave.Constants.selectorCountFields,
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
            grid: false,
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
            step: [Ext.Date.MINUTE, Datawave.Constants.queryIntervalMinutes]
        }
    ],
    series: [
        {
            axis: 'left',
            type: 'column',
            xField: 'dateTime',
            yField: Datawave.Constants.selectorCountFields,
            title: Datawave.Constants.selectorCountTitles,
            stacked: true,
            style: {
                opacity: Datawave.Constants.opacity
            }
        }
    ]
});

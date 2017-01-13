Ext.define('Datawave.view.ingest.IngestLiveEventChart', {
    extend: 'Ext.chart.Chart',
    store: 'IngestStats',
    alias: 'widget.ingestliveeventchart',
    animate: true,
    shadow: true,
    style: 'background-color: #fff',
    axes: [
        {
            type: 'Numeric',
            position: 'left',
            fields: ['liveEventCount'],
            minimum: 0,
            grid: true,
            label: {
                renderer: Ext.util.Format.numberRenderer('0,0')
            },
            hidden:false
        },
        {
            type: 'Time',
            position: 'bottom',
            fields: ['dateTime'],
            label: {
                renderer: function(v) {
                    var date = new Date(v),
                            hours = date.getHours();
                    if (date.getMinutes() !== 0) {
                        return '';
                    }

                    else if (hours === 0) {
                        return Ext.util.Format.date(date, 'Y/m/d');
                    } 
                    
                    else if (hours === 3) {
                        return '0300';
                    } 
                    
                    else if (hours === 6) {
                        return '0600';
                    } 
                    
                    else if (hours === 9) {
                        return '0900';
                    } 
                    
                    else if (hours === 12) {
                        return '1200';
                    }
                    
                    else if (hours === 15) {
                        return '1500';
                    }
                    
                    else if (hours === 18) {
                        return '1800';
                    }
                    
                    else if (hours === 21) {
                        return '2100';
                    }
                    
                    return '';
                }
            },
            grid: false,
            step: [Ext.Date.MINUTE, Datawave.Constants.ingestIntervalMinutes]
        }
    ],
    series: [
        {
            type: 'column',
            xField: 'dateTime',
            yField: 'liveEventCount',
            style: {
                opacity: Datawave.Constants.opacity
            }
        }
    ]
});

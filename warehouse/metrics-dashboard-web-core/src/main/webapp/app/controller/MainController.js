var windowHours = 96,
        preloaded = false, //get last 96 hours, one at a time,
        mcRangeMs = 60 * 60 * 1000; // 1 hour in milliseconds 

Ext.define('Datawave.controller.MainController', {
    extend: 'Ext.app.Controller',
    init: function(app) {
        var me = this,
                store = app.getQueryStatsStore();

        store.on('load', me.onQueryStatsStoreLoad, me);
        this.updateStore(store);
    },
    onQueryStatsStoreLoad: function(store, records, success) {
        if (success === false) {
            this.showError('An error occurred loading startup query statistics.');
        } else {
            this.trimStore(store, windowHours);
            this.updateBarStores(store);
        }

        if (preloaded === false) {
            this.updateStore(store);
        } else {//auto-adjust to running at the top of the hour
            var now = new Date(),
                    nowMin = now.getMinutes(),
                    topOfHourMin = 60 - nowMin,
                    delayMs = topOfHourMin * 60 * 1000;

            var me = this; // work around scope issue
            new Ext.util.DelayedTask(function() {
                me.updateStore(store);
            }).delay(delayMs);
        }
    },
    initializeTitleArray: function(titles) {
        return [
            [titles[0], 0],
            [titles[1], 0],
            [titles[2], 0],
            [titles[3], 0],
            [titles[4], 0]
        ];
    },
    updateStore: function(store) {
        var now = new Date();
        now.setMilliseconds(0);
        now.setSeconds(0);
        now.setMinutes(0);
        var hoursAgo = windowHours - store.getCount() + 1,
                start = now - (hoursAgo * mcRangeMs);

        store.load({
            params: {
                start: start,
                end: start + mcRangeMs
            },
            addRecords: true
        });
    },
    updateBarStores: function(store) {
        var queryData = this.initializeTitleArray(Datawave.Constants.queryCountTitles);
        var resultData = this.initializeTitleArray(Datawave.Constants.resultCountTitles);
        var selectorData = this.initializeTitleArray(Datawave.Constants.selectorCountTitles);

        store.each(function(rec) {
            queryData[0][1] += rec.get('upTo3Sec');
            queryData[1][1] += rec.get('upTo10Sec');
            queryData[2][1] += rec.get('upTo60Sec');
            queryData[3][1] += rec.get('moreThan60Sec');
            queryData[4][1] += rec.get('errorCount');

            resultData[0][1] += rec.get('upTo10KResults');
            resultData[1][1] += rec.get('upTo1MResults');
            resultData[2][1] += rec.get('upToINFResults');
            resultData[3][1] += rec.get('zeroResults');
            resultData[4][1] += rec.get('errorCount');

            selectorData[0][1] += rec.get('oneTerm');
            selectorData[1][1] += rec.get('upTo16Terms');
            selectorData[2][1] += rec.get('upTo100Terms');
            selectorData[3][1] += rec.get('upTo1000Terms');
            selectorData[4][1] += rec.get('upToInfTerms');
        });

        Ext.data.StoreManager.get('queryBarStore').loadData(queryData);
        Ext.data.StoreManager.get('resultBarStore').loadData(resultData);
        Ext.data.StoreManager.get('selectorBarStore').loadData(selectorData);
    },
    showError: function(msg) {
        if (Datawave.Constants.DEBUG === true) {
            Ext.Msg.show({
                msg: msg,
                width: 300,
                buttons: Ext.Msg.OK,
                title: 'Server error'
            });
        }
    },
    trimStore: function(store, max) {
        var count = store.getCount();
        if (count > max) {
            preloaded = true;//we're done back-loading data
            store.removeAt(0, (count - max));
        }
    }
});

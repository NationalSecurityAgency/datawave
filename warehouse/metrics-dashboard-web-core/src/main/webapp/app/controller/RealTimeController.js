var rangeMs = 30000,
        maxEntries = 120;

/**
 * This controller will continuously load data every 30 seconds but will not 
 * repeat a request until the previous request has completed or timed out.
 */
Ext.define('Datawave.controller.RealTimeController', {
    extend: 'Ext.app.Controller',
    init: function() {

        var me = this,
                store = Ext.data.StoreManager.get('realTimeStore');

        store.on('load', me.onQueryStatsStoreLoad, me);
        this.updateStore(store);
    },
    onQueryStatsStoreLoad: function(store, records, success) {
        if (success === false) {
            this.showError('An error occurred loading real-time query statistics.');
        } else {
            this.trimStore(store, maxEntries);
        }

        var me = this;  // work around scope issue
        new Ext.util.DelayedTask(function() {
            me.updateStore(store);
        }).delay(rangeMs);
    },
    updateStore: function(store) {
        var now = new Date().getTime();

        store.load({
            params: {
                start: now - rangeMs,
                end: now
            },
            addRecords: true
        });
    },
    trimStore: function(store, max) {
        var count = store.getCount();
        if (count > max) {
            store.removeAt(0, (count - max));
        }
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
    }
});
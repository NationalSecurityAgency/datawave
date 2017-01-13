Ext.define('Datawave.controller.IngestController', {
    extend: 'Ext.app.Controller',
    init: function(app) {
        var ingestStore = app.getIngestStatsStore();

        Ext.TaskManager.start({
            run: function() {
                ingestStore.load({
                    callback: function(operation,records, success) {
                        if (success === false) {
                            Ext.Msg.show({
                                msg: 'An error occurred loading ingest statistics.',
                                width: 300,
                                buttons: Ext.Msg.OK,
                                title: 'Server error'
                            });
                        }
                    },
                    addRecords: false // replace existing data
                });
            },
            interval: 15 * 60 * 1000 // 15 minutes
        });
    }
});

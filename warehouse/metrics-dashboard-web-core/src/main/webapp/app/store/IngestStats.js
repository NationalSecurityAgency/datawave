Ext.define('Datawave.store.IngestStats', {
    extend: 'Ext.data.Store',
    model: 'Datawave.model.IngestStat',
    autoLoad: false,
    proxy: {
        url: Datawave.Constants.ingestURL,
        type: 'ajax',
        reader: {
            type: 'json',
            root: 'data'
        }
    }
});

Ext.define('Datawave.store.QueryStats', {
    extend: 'Ext.data.Store',
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

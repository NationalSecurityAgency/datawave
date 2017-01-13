Ext.define('Datawave.model.QueryStat', {
    extend: 'Ext.data.Model',
    idField: 'dateTime',
    fields: [
        {
            name: 'dateTime',
            dateFormat: 'u'
        },
        //latencies
        'upTo3Sec',
        'upTo10Sec',
        'upTo60Sec',
        'moreThan60Sec',
        'errorCount',
        //results
        'zeroResults',
        'upTo10KResults',
        'upTo1MResults',
        'upToINFResults',
        //terms
        'oneTerm',
        'upTo16Terms',
        'upTo100Terms',
        'upTo1000Terms',
        'upToInfTerms',
        //totals
        'resultCount',
        'queryCount',
        'selectorCount'
    ]
});

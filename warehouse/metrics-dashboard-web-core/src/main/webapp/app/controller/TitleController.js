Ext.define('Datawave.controller.TitleController', {
    extend: 'Ext.app.Controller',
    init: function() {
        this.updateConnectionText(!Datawave.Constants.sanitize);
    },
    updateConnectionText: function(displaySystemName) {
        var queryStr = String(document.location);
        if (queryStr.indexOf('https://localhost') === 0) {
            this.updateTitle(displaySystemName, 'LOCALHOST', 'DEVELOPMENT');
	    var parts = queryStr.split("/");
            var fullyQualifiedName = parts[2];	
            var hostname = fullyQualifiedName.split(".")[0];
            this.updateTitle(hostname, '', 'DEVELOPMENT');
        } else {
	    var parts = queryStr.split("/");
            var fullyQualifiedName = parts[2];	
            var hostname = fullyQualifiedName.split(".")[0];
            this.updateTitle(hostname, '', 'DEVELOPMENT');
        }
    },
    updateTitle: function(displaySystemName, systemName, type) {
        if (displaySystemName) {
            systemName = systemName + ' ';
        } else {
            systemName = '';
        }
        var title2 = ' System Dashboard | Datawave';
        Ext.getDoc().dom.title = systemName + type + title2;
    }
});

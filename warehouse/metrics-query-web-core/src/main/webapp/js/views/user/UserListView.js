define([
  'jquery',
  'underscore',
  'backbone',
  'collections/user/UserCollection',
  'collections/metrics/user/UserQueryMetricSummaryCollection',
  'text!templates/user/userListTemplate.html',
  'text!templates/user/listTemplate.html',
], function($, _, Backbone, UserCollection, UserQueryMetricSummaryCollection,  userListTemplate, listTemplate) {

  var UserView = Backbone.View.extend({
    
    className: 'user-list-container well',
    
    events: {
      'click ul > li > a': '_selectUser',
      'change .user-list-input': '_inputChange',
      'keyup .user-list-input': '_inputChange'
    },
    
    initialize: function() {
      _.bindAll(this);
      var that = this;
    },
    
    render: function() {
      
      var el = this.$el;
    
      var compiledTemplate = _.template(userListTemplate);
      el.append(compiledTemplate);
      
      return this;
    },
    
    setRange: function(d) {
      
      var beginString, endString;
      
      var begin = d[0];
      var end   = d[1];
      
      if(_.isDate(begin)) {
        beginString = begin.getUTCFullYear() +
                        ('0' + (begin.getUTCMonth() + 1)).slice(-2) + 
                        ('0' + (begin.getUTCDate())).slice(-2);
      }
                        
      if(_.isDate(end)) {       
        endString = end.getUTCFullYear() +
                      ('0' + (end.getUTCMonth() + 1)).slice(-2) + 
                      ('0' + (end.getUTCDate())).slice(-2);
      }
      
      if(this.jqXHR) {
        this.jqXHR.abort();
        
        this._begin = null;
        this._end   = null;
      }
      
      if(this._begin != beginString || this._end != endString) {
        this._begin = beginString;
        this._end   = endString;
        this._loading();
        this.collection = new UserQueryMetricSummaryCollection();
        this.jqXHR = this.collection.fetch({data: {begin: beginString, end: endString}, success: this._success});
      }
    },
    
    _success: function(collection, response, options) {
      console.log(collection);
      
      if(this.jqXHR) this.jqXHR = null;
      
      var userCollection = new UserCollection();
      
      collection.each(function(obj) {
        userCollection.add({id: obj.get('User')});
      }, this);
      
      var idList = userCollection.pluck('id');
      
      var uniqIdList = _.uniq(idList);
      
      this._users = uniqIdList;
      
      var el = this.$el;
      var container = el.find('.user-list-inner');
      
      el.find('.user-list-loading').addClass('hide');
      container.removeClass('hide');
      this._updateUsers(container, this._users);
    },
    
    _error: function(model, xhr, options) {
      this.$el.find('.user-list-loading').addClass('hide');
      this.$el.find('.user-list-inner').addClass('hide');
      this.$el.find('.user-list-error').removeClass('hide');
    },
    
    _loading: function() {
      this.$el.find('.user-list-loading').removeClass('hide');
      this.$el.find('.user-list-inner').addClass('hide');
      this.$el.find('.user-list-error').addClass('hide');
    },
    
    _updateUsers: function(container, collection) {
      
      container.empty();
      
      var list = $(listTemplate);
      
      _.each(collection, function(user) {
        var item = $(document.createElement('li'));
        var anchor = $(document.createElement('a'));
        
        if (user === this._user)
          item.addClass('active');
        
        list.append(item.append(anchor.text(user)));
      }, this);
      
      container.append(list);
    },
    
    _selectUser: function(event) {
      console.log(event);
      var target = $(event.currentTarget);
      this.user(target.text().trim());
      
      var ul = target.closest('ul:not(.dropdown-menu)');
      var selector = ul.children('li.active');
      
      selector.removeClass('active');
      target.closest('li').addClass('active');
      
      this.$el.trigger('selectUser');
    },
    
    user: function(_) {
      if(!arguments.length) return this._user;
      this._user = _;
      this._updateSelect();
      return this._user;
    },
    
    _updateSelect: function() {
      var el = this.$el;
      
      var container = el.find('.user-list-inner');
      container.find('li.active').removeClass('active');
      
      _.each(container.find('li'), function(item) {
        var target = $(item);
        if(target.text().trim() == this.user)
          target.addClass('active');
      }, this);
      
      return this;
    },
    
    _inputChange: function(event) {
      event.stopPropagation();
      var target = $(event.currentTarget);
      var text = target.val().toLowerCase();
      
      var list = [];
      _.each(this._users, function(user) {
        if(user.toLowerCase().indexOf(text) != -1)
          list.push(user);
      }, this);
      
      var el = this.$el;
      var container = el.find('.user-list-inner');
      
      this._updateUsers(container, list)
    }
  });
  
  return UserView;
});

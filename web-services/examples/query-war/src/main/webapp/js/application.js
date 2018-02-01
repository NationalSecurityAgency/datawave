var datawave = datawave || {};

var setupAccordionLike = function() {
    // extend accordion to allow multiple sections to be open at a time
    $(function() {
        $("#accordion").addClass("ui-accordion ui-widget ui-helper-reset")
        .find("h3")
          .addClass("ui-accordion-header ui-helper-reset ui-state-default ui-corner-top ui-corner-bottom")
          .prepend('<span class="ui-icon ui-icon-triangle-1-e"/>')
          .click(function() {
            $(this).toggleClass('ui-accordion-header-active').toggleClass('ui-state-active')
              .toggleClass("ui-state-default").toggleClass("ui-corner-bottom")
            .find("> .ui-icon").toggleClass("ui-icon-triangle-1-e").toggleClass("ui-icon-triangle-1-s")
            .end().next().toggleClass("ui-accordion-content-active").toggle();
            return false

          })
          .next().addClass("ui-accordion-content ui-helper-reset ui-widget-content ui-corner-bottom").hide();
    });
};

var setupFormHandlers = function() {
    createFormHandler();
    listIdFormHandler();
    listFormHandler();
    executeFormHandler();
    closeFormHandler();
    cancelFormHandler();
    removeFormHandler();
};

var createFormHandler = function() {
    // take the form and submit with AJAX
    $('#create-form').submit(function() {
          displayResultSpinner('#create-result');
          // get the name of the query so we can use it later
          datawave.queryName = $(this).find('input:text[name=queryName]').val();
          $.ajax({
            type: 'POST',
            url: $(this).attr('action'),
            data: $(this).serialize(),
            success:  function(data) {
              datawave.queryId = data.result;
              //alert("Query id is " + datawave.queryId);
              displayResult(data, '#create-result');
              displayQueryId();
              populateOtherForms();
            },
            error: function(jqXHR, textStatus, errorThrown) {
              datawave.queryName = null;
              datawave.queryId = null;
              populateOtherForms();
              displayResult(jqXHR, '#create-result');
            }
          });
          return false;
    });
    $('#populate-example').click(function() {
      populateCreateSampleForm();
    });
};

var displayQueryId = function() {
  $('#query').first().html("Query : current id = " + datawave.queryId);
}

var displayResultSpinner = function(target) {
  $(target).html("<h2>Result: <img src='/DataWave/Examples/images/spinner.gif'/></h2>");
}

var displayResult = function(result, target) {
  $(target).html("<h2>Result:</h2><pre><code>"+JSON.stringify(result, null, 2)+"</code></pre>");
}

var populateCreateSampleForm = function() {
  var cform = $('#create-form');

  var now = new Date();
  var future = new Date();
  now.setDate(now.getDate() - 30);

  cform.find('input:text[name=queryName]').val("Test query " + jQuery.now());
  cform.find('input:text[name=query]').val("FOO == 'FOOFIELDVALUE'");
  cform.find('select[name=logicName] option[value=EventQuery]').attr('selected','selected');
  cform.find('input:text[name=pagesize]').val(10);
  cform.find('input:text[name=auths]').val('');
  cform.find('input:text[name=columnVisibility]').val('PUBLIC');
  cform.find('input:text[name=end]').val($.datepicker.formatDate('yymmdd',now).toString());
  cform.find('input:text[name=begin]').val($.datepicker.formatDate('yymmdd',future).toString());
  cform.find('input:text[name=expiration]').val(expire30days());
}

var expire30days = function() {
  var now = new Date();
  now.setDate(now.getDate() + 30);
  return $.datepicker.formatDate('yymmdd',now).toString();
}

var populateOtherForms = function() {
  $('#list-by-id').find('input:text[name=queryId]').val(datawave.queryId);
  $('#list-by-name').find('input:text[name=name]').val(datawave.queryName);
  $('#execute-query').find('input:text[name=queryId]').val(datawave.queryId);
  $('#close-query').find('input:text[name=queryId]').val(datawave.queryId);
  $('#cancel-query').find('input:text[name=queryId]').val(datawave.queryId);
  $('#remove-query').find('input:text[name=queryId]').val(datawave.queryId);
}

var listIdFormHandler = function() {
  $('#list-by-id').submit(function() {
    var submittedId = $(this).find('input:text[name=queryId]').val();
    displayResultSpinner('#list-id-result');
    $.ajax({
      type: 'GET',
      url: "/DataWave/Query/"+submittedId+".json",
      data: $(this).serialize(),
      success:  function(data) {
        displayResult(data, '#list-id-result');
      },
      error: function(jqXHR, textStatus, errorThrown) {
        displayResult(jqXHR, '#list-id-result');
      }
    });
    return false;
  });
}

var listFormHandler = function() {
  $('#list-by-name').submit(function() {
    displayResultSpinner('#list-result');
    $.ajax({
      type: 'GET',
      url: $(this).attr('action'),
      data: $(this).serialize(),
      success:  function(data) {
        displayResult(data, '#list-result');
      },
      error: function(jqXHR, textStatus, errorThrown) {
        displayResult(jqXHR, '#list-result');
      }
    });
    return false;
  });
}


var executeFormHandler = function() {
  $('#execute-query').submit(function() {
    var submittedId = $(this).find('input:text[name=queryId]').val();
    displayResultSpinner('#execute-result');
    $.ajax({
      type: 'GET',
      url: "/DataWave/Query/"+submittedId+"/next.json",
      data: $(this).serialize(),
      success:  function(data) {
        displayResult(data, '#execute-result');
      },
      error: function(jqXHR, textStatus, errorThrown) {
        displayResult(jqXHR, '#execute-result');
      }
    });
    return false;
  });
}

var closeFormHandler = function() {
  $('#close-query').submit(function() {
    var submittedId = $(this).find('input:text[name=queryId]').val();
    displayResultSpinner('#close-result');
    $.ajax({
      type: 'PUT',
      url: "/DataWave/Query/"+submittedId+"/close.json",
      data: $(this).serialize(),
      success:  function(data) {
        displayResult(data, '#close-result');
      },
      error: function(jqXHR, textStatus, errorThrown) {
        displayResult(jqXHR, '#close-result');
      }
    });
    return false;
  });
}

var cancelFormHandler = function() {
  $('#cancel-query').submit(function() {
    var submittedId = $(this).find('input:text[name=queryId]').val();
    displayResultSpinner('#cancel-result');
    $.ajax({
      type: 'PUT',
      url: "/DataWave/Query/"+submittedId+"/cancel.json",
      data: $(this).serialize(),
      success:  function(data) {
        displayResult(data, '#cancel-result');
      },
      error: function(jqXHR, textStatus, errorThrown) {
        displayResult(jqXHR, '#cancel-result');
      }
    });
    return false;
  });
}
var removeFormHandler = function() {
  $('#remove-query').submit(function() {
    var submittedId = $(this).find('input:text[name=queryId]').val();
    displayResultSpinner('#remove-result');
    $.ajax({
      type: 'DELETE',
      url: "/DataWave/Query/"+submittedId+"/remove.json",
      data: $(this).serialize(),
      success:  function(data) {
        displayResult(data, '#remove-result');
      },
      error: function(jqXHR, textStatus, errorThrown) {
        displayResult(jqXHR, '#remove-result');
      }
    });
    return false;
  });
}

jQuery(document).ready(function() {
    setupAccordionLike();
    setupFormHandlers();
});

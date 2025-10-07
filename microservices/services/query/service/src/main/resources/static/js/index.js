    $(document).foundation()

// This code expects the following templated values to be set in index.html
// var buildUserEmail = "[[${gitBuildUserEmail}]]";
// var branch = "[[${gitBranch}]]";
// var commitId = "[[${gitCommitId}]]";
// var buildTime = "[[${gitBuildTime}]]";

// Set additional content when ready
$(function() {
    var gitInfoHtml = 'Built By: ' + buildUserEmail + '<br/>' + 
                      'Branch: ' + branch + '<br/>' + 
                      'Commit: ' + commitId + '<br/>' + 
                      'Date/Time: ' + buildTime;

    $("#gitInfo").html(gitInfoHtml);
});

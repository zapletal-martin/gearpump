function login() {

    var loginUrl = $("#loginUrl").attr('href');
    var index = $("#index").attr('href');

    $.post(loginUrl, $("#loginForm").serialize() ).done(
      function(msg) {
          var user = $.parseJSON(msg);
          $.cookie("username", user.user, { expires: 365, path: '/' });
          // clear the errors
          $("#error").text("");
          // redirect to index.html
          $(location).attr('href', index);
      }
    )
    .fail( function(xhr, textStatus, errorThrown) {
       $("#error").text(textStatus + "(" + xhr.status + "): " + xhr.responseText);
    });
}

function logout() {
    var logoutUrl = $("#logoutUrl").attr('href');
    $.post(logoutUrl)
}

$(document).ready(function() {
    logout();
});
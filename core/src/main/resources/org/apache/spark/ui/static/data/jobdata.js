
window.onload(function() {
    $("#datatabiframe").on("load", function() {
        // var frame = document.getElementById('myiframe');
        var frame = $("#myiframe");
        frame.on("load", function() {
            frame.contentWindow.postMessage("this is my message", '*');
        });
    });
});

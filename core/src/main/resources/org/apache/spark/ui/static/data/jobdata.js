
$(function () {
    myproperty = 100;
});
myanotherproperty = 200;
window.onload(function() {
    window.addEventListener('message',function(e){
        if (e.data["eventType"] !== "iframeready") {
            return;
        }
        window.postMessage({
            eventType: "startRender",
            event: "start"
        }, "localhost:5555")
    },false);
});

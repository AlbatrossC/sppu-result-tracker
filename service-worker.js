self.addEventListener("push", event => {
    const data = event.data ? event.data.json() : {};

    const title = data.title || "📢 SPPU Result Update";
    const options = {
        body: data.body || "A new result update is available!",
        icon: "/icons/icon-192.png",
        badge: "/icons/icon-192.png",
        vibrate: [200, 100, 200],
        tag: "sppu-result-update",
        renotify: true
    };

    event.waitUntil(
        self.registration.showNotification(title, options)
    );
});

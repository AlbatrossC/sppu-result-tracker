importScripts("https://www.gstatic.com/firebasejs/12.6.0/firebase-app-compat.js");
importScripts("https://www.gstatic.com/firebasejs/12.6.0/firebase-messaging-compat.js");

firebase.initializeApp({
    apiKey: "AIzaSyACkJ8BkOMwELdo-y1e4uu2HDEiU09xZ8I",
    authDomain: "sppu-result-tracker.firebaseapp.com",
    projectId: "sppu-result-tracker",
    storageBucket: "sppu-result-tracker.firebasestorage.app",
    messagingSenderId: "643791611590",
    appId: "1:643791611590:web:d641fbab6e77a4c88959c7"
});

const messaging = firebase.messaging();

messaging.onBackgroundMessage((payload) => {
    const title = payload.notification?.title || "SPPU Result";
    const body = payload.notification?.body || "New update available";

    self.registration.showNotification(title, {
        body: body,
        icon: "/icon.png",
        badge: "/icon.png",
        vibrate: [100, 50, 100],
        requireInteraction: false,
        data: payload.data || {}
    });
});

self.addEventListener("notificationclick", (event) => {
    event.notification.close();
    event.waitUntil(
        clients.matchAll({ includeUncontrolled: true, type: "window" })
            .then((list) => {
                for (const c of list) {
                    if (c.url.includes("/") && "focus" in c) return c.focus();
                }
                if (clients.openWindow) return clients.openWindow("/");
            })
    );
});

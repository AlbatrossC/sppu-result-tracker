// Import Firebase scripts for service worker (compat version is required)
importScripts("https://www.gstatic.com/firebasejs/12.6.0/firebase-app-compat.js");
importScripts("https://www.gstatic.com/firebasejs/12.6.0/firebase-messaging-compat.js");

// Initialize Firebase inside the service worker
firebase.initializeApp({
    apiKey: "AIzaSyACkJ8BkOMwELdo-y1e4uu2HDEiU09xZ8I",
    authDomain: "sppu-result-tracker.firebaseapp.com",
    projectId: "sppu-result-tracker",
    storageBucket: "sppu-result-tracker.firebasestorage.app",
    messagingSenderId: "643791611590",
    appId: "1:643791611590:web:d641fbab6e77a4c88959c7"
});

// Retrieve Firebase Messaging instance
const messaging = firebase.messaging();

// Handle background messages (when the webpage is closed)
messaging.onBackgroundMessage((payload) => {
    console.log("📨 Received background message:", payload);

    const notificationTitle = payload.notification?.title || "SPPU Result Update";
    const notificationBody = payload.notification?.body || "New update available.";
    const notificationIcon = "/icon.png"; // Replace with your icon if needed

    self.registration.showNotification(notificationTitle, {
        body: notificationBody,
        icon: notificationIcon,
        data: payload.data || {}
    });
});

// Optional: Handle notification click (open website)
self.addEventListener("notificationclick", function (event) {
    event.notification.close();

    event.waitUntil(
        clients.matchAll({ type: "window", includeUncontrolled: true }).then(windowClients => {
            for (let client of windowClients) {
                if (client.url.includes("/") && "focus" in client) {
                    return client.focus();
                }
            }
            if (clients.openWindow) {
                return clients.openWindow("/");
            }
        })
    );
});

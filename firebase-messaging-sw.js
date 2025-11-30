// Import Firebase SDKs for service worker
importScripts("https://www.gstatic.com/firebasejs/12.6.0/firebase-app-compat.js");
importScripts("https://www.gstatic.com/firebasejs/12.6.0/firebase-messaging-compat.js");

// Initialize Firebase with project configuration
firebase.initializeApp({
    apiKey: "AIzaSyACkJ8BkOMwELdo-y1e4uu2HDEiU09xZ8I",
    authDomain: "sppu-result-tracker.firebaseapp.com",
    projectId: "sppu-result-tracker",
    storageBucket: "sppu-result-tracker.firebasestorage.app",
    messagingSenderId: "643791611590",
    appId: "1:643791611590:web:d641fbab6e77a4c88959c7"
});

// Get Firebase messaging instance
const messaging = firebase.messaging();

// Handle incoming background messages when app is not in focus
messaging.onBackgroundMessage((payload) => {
    console.log("[Service Worker] Background message received:", payload);
    
    // Extract notification title and body from payload
    const title = payload.notification?.title || "SPPU Result Update";
    const body = payload.notification?.body || "Check out the latest result updates";
    
    // Create notification options with enhanced features
    const notificationOptions = {
        body: body,
        icon: "/icon.png",
        badge: "/icon.png",
        vibrate: [200, 100, 200], // Vibration pattern: vibrate-pause-vibrate
        tag: "sppu-result-notification", // Replace existing notifications with same tag
        renotify: true, // Notify even if tag already exists
        requireInteraction: false, // Auto-dismiss after timeout
        data: {
            url: "/", // URL to open when clicked
            timestamp: payload.data?.timestamp || new Date().toISOString(),
            ...payload.data
        },
        actions: [
            {
                action: "view",
                title: "View Results"
            },
            {
                action: "dismiss",
                title: "Dismiss"
            }
        ]
    };
    
    console.log("[Service Worker] Showing notification:", title);
    
    // Display the notification to user
    return self.registration.showNotification(title, notificationOptions);
});

// Handle notification click events
self.addEventListener("notificationclick", (event) => {
    console.log("[Service Worker] Notification clicked:", event.action);
    
    // Close the notification
    event.notification.close();
    
    // Handle different action buttons
    if (event.action === "dismiss") {
        console.log("[Service Worker] Notification dismissed by user");
        return;
    }
    
    // Get the URL to open from notification data
    const urlToOpen = event.notification.data?.url || "/";
    
    // Open or focus the app window
    event.waitUntil(
        clients.matchAll({ 
            includeUncontrolled: true, 
            type: "window" 
        }).then((clientList) => {
            console.log("[Service Worker] Found", clientList.length, "open window(s)");
            
            // Try to focus an existing window first
            for (const client of clientList) {
                if (client.url === new URL(urlToOpen, self.location.origin).href && "focus" in client) {
                    console.log("[Service Worker] Focusing existing window");
                    return client.focus();
                }
            }
            
            // If no matching window found, open a new one
            if (clients.openWindow) {
                console.log("[Service Worker] Opening new window:", urlToOpen);
                return clients.openWindow(urlToOpen);
            }
        }).catch((error) => {
            console.error("[Service Worker] Error handling notification click:", error);
        })
    );
});

// Handle notification close events for analytics
self.addEventListener("notificationclose", (event) => {
    console.log("[Service Worker] Notification closed:", event.notification.tag);
});

// Service worker activation - clean up old caches if needed
self.addEventListener("activate", (event) => {
    console.log("[Service Worker] Activated");
    event.waitUntil(clients.claim());
});

// Log service worker installation
self.addEventListener("install", (event) => {
    console.log("[Service Worker] Installed");
    self.skipWaiting();
});
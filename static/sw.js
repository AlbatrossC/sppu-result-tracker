// Service Worker for handling push notifications

self.addEventListener('install', function(event) {
    console.log('Service Worker installed');
    self.skipWaiting();
});

self.addEventListener('activate', function(event) {
    console.log('Service Worker activated');
    event.waitUntil(self.clients.claim());
});

self.addEventListener('push', function(event) {
    console.log('Push received:', event);
    
    let notificationData = {
        title: 'SPPU Result Update',
        body: 'You have a new result notification',
        icon: '/static/icon.png',
        badge: '/static/badge.png',
        data: { url: '/' }
    };
    
    if (event.data) {
        try {
            notificationData = { ...notificationData, ...event.data.json() };
        } catch (e) {
            console.error('Error parsing notification data:', e);
            // Use default data if parsing fails
        }
    }
    
    const promiseChain = self.registration.showNotification(
        notificationData.title,
        {
            body: notificationData.body,
            icon: notificationData.icon,
            badge: notificationData.badge,
            data: notificationData.data,
            vibrate: [200, 100, 200],
            tag: 'sppu-result-notification',
            requireInteraction: false,
            actions: [
                {
                    action: 'open',
                    title: 'View Results'
                },
                {
                    action: 'close',
                    title: 'Dismiss'
                }
            ]
        }
    );
    
    event.waitUntil(promiseChain);
});

self.addEventListener('notificationclick', function(event) {
    console.log('Notification clicked:', event);
    
    event.notification.close();
    
    if (event.action === 'close') {
        return;
    }
    
    const urlToOpen = event.notification.data?.url || '/';
    
    event.waitUntil(
        clients.matchAll({
            type: 'window',
            includeUncontrolled: true
        })
        .then(function(clientList) {
            // Check if there's already a window open
            for (let i = 0; i < clientList.length; i++) {
                const client = clientList[i];
                if (client.url.includes(urlToOpen) && 'focus' in client) {
                    return client.focus();
                }
            }
            // If not, open a new window
            if (clients.openWindow) {
                return clients.openWindow(urlToOpen);
            }
        })
    );
});

self.addEventListener('pushsubscriptionchange', function(event) {
    console.log('Subscription changed:', event);
    
    event.waitUntil(
        self.registration.pushManager.subscribe({
            userVisibleOnly: true,
            applicationServerKey: event.oldSubscription.options.applicationServerKey
        })
        .then(function(subscription) {
            console.log('Resubscribed:', subscription);
            // Send new subscription to server
            return fetch('/api/resubscribe', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    old_subscription: event.oldSubscription.toJSON(),
                    new_subscription: subscription.toJSON()
                })
            });
        })
    );
});
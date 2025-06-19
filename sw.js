self.addEventListener('push', function(event) {
  const data = event.data.json();
  const options = {
    body: data.body,
    icon: data.icon || '/assest-src/512x512.png',
    data: { url: data.url }
  };
  event.waitUntil(self.registration.showNotification(data.title, options));
});

self.addEventListener('notificationclick', function(event) {
  event.notification.close();
  const url = event.notification.data.url;
  event.waitUntil(clients.matchAll({ type: 'window', includeUncontrolled: true }).then(function(clientList) {
    let matchingClient = null;
    for (let i = 0; i < clientList.length; i++) {
      const client = clientList[i];
      if (client.url === url && 'focus' in client) {
        matchingClient = client;
        break;
      }
    }
    if (matchingClient) {
      return matchingClient.navigate(url).then(function(client) { return client.focus(); });
    } else {
      return clients.openWindow(url);
    }
  }));
});
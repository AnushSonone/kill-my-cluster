import type { Handle } from '@sveltejs/kit';

const CP = process.env.CONTROLPLANE_URL || 'http://127.0.0.1:8080';

/** Proxy /api/* to the control plane (works in Docker + local preview). */
export const handle: Handle = async ({ event, resolve }) => {
  if (!event.url.pathname.startsWith('/api/')) {
    return resolve(event);
  }

  const target = new URL(event.url.pathname + event.url.search, CP);
  const headers = new Headers(event.request.headers);
  headers.delete('host');

  const init: RequestInit & { duplex?: string } = {
    method: event.request.method,
    headers,
    duplex: 'half'
  };

  if (event.request.method !== 'GET' && event.request.method !== 'HEAD') {
    init.body = event.request.body;
  }

  try {
    const res = await fetch(target, init);
    return new Response(res.body, {
      status: res.status,
      statusText: res.statusText,
      headers: res.headers
    });
  } catch (err) {
    return new Response(`control plane unreachable: ${err}`, { status: 502 });
  }
};

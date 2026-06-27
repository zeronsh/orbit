// Minimal production server for the TanStack Start build: serve dist/client static
// assets, and pass everything else to the SSR fetch handler (dist/server/server.js).
import { createServer } from 'node:http';
import { readFile } from 'node:fs/promises';
import { existsSync, statSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { join, extname } from 'node:path';
import app from './dist/server/server.js';

const clientDir = fileURLToPath(new URL('./dist/client/', import.meta.url));
const PORT = Number(process.env.PORT || 3000);
const MIME = {
  '.js': 'text/javascript', '.mjs': 'text/javascript', '.css': 'text/css',
  '.html': 'text/html', '.json': 'application/json', '.svg': 'image/svg+xml',
  '.png': 'image/png', '.jpg': 'image/jpeg', '.ico': 'image/x-icon',
  '.woff2': 'font/woff2', '.woff': 'font/woff', '.map': 'application/json', '.txt': 'text/plain',
};

function readBody(req) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    req.on('data', (c) => chunks.push(c));
    req.on('end', () => resolve(Buffer.concat(chunks)));
    req.on('error', reject);
  });
}

createServer(async (req, res) => {
  try {
    const pathname = decodeURIComponent(new URL(req.url, 'http://x').pathname);
    // static client asset?
    if (pathname !== '/' && !pathname.includes('..')) {
      const fp = join(clientDir, pathname);
      if (existsSync(fp) && statSync(fp).isFile()) {
        const data = await readFile(fp);
        res.writeHead(200, {
          'content-type': MIME[extname(fp)] || 'application/octet-stream',
          'cache-control': pathname.startsWith('/assets/') ? 'public, max-age=31536000, immutable' : 'no-cache',
        });
        res.end(data);
        return;
      }
    }
    // SSR via the web fetch handler
    const headers = new Headers();
    for (const [k, v] of Object.entries(req.headers)) {
      if (typeof v === 'string') headers.set(k, v);
      else if (Array.isArray(v)) headers.set(k, v.join(', '));
    }
    const url = `http://${req.headers.host || 'localhost'}${req.url}`;
    const hasBody = !['GET', 'HEAD'].includes(req.method);
    const request = new Request(url, {
      method: req.method,
      headers,
      body: hasBody ? await readBody(req) : undefined,
      duplex: 'half',
    });
    const response = await app.fetch(request);
    res.writeHead(response.status, Object.fromEntries(response.headers));
    if (response.body) {
      const reader = response.body.getReader();
      for (;;) {
        const { done, value } = await reader.read();
        if (done) break;
        res.write(value);
      }
    }
    res.end();
  } catch (e) {
    res.writeHead(500, { 'content-type': 'text/plain' });
    res.end('Internal Error: ' + (e?.stack || e?.message || e));
  }
}).listen(PORT, '::', () => console.log(`orbit-issues app listening on :${PORT}`));

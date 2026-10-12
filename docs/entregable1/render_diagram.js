// Exporta un .drawio a PNG (2x) y SVG con el visor oficial de draw.io (incluye los íconos AWS).
// Requiere una copia de jgraph/drawio (src/main/webapp) en DRAWIO_WEBAPP y playwright-core.
// Uso: DRAWIO_WEBAPP=/ruta/drawio/src/main/webapp node render_diagram.js arquitectura.drawio salida.png salida.svg
const { chromium } = require('playwright-core');
const http = require('http');
const fs = require('fs');
const path = require('path');

const [drawio, outPng, outSvg] = process.argv.slice(2);
const ROOT = process.env.DRAWIO_WEBAPP;
const xml = fs.readFileSync(drawio, 'utf8');
const W = +xml.match(/pageWidth="(\d+)"/)[1];
const H = +xml.match(/pageHeight="(\d+)"/)[1];

const page = `<!doctype html><html><head><meta charset="utf-8">
<style>body{margin:0;background:#fff} #g{width:${W}px;height:${H}px}</style>
<script>window.STENCIL_PATH='stencils';window.SHAPES_PATH='shapes';window.mxLoadResources=false;</script>
<script src="js/viewer-static.min.js"></script>
<script src="js/stencils.min.js"></script>
<script src="shapes/mxAWS4.js"></script>
</head><body><div id="g"></div><script>
const doc = mxUtils.parseXml(${JSON.stringify(xml)});
const viewer = new GraphViewer(document.getElementById('g'), doc.documentElement,
  {toolbar: null, nav: false, resize: false, border: 0, lightbox: false, center: false});
window.viewer = viewer; window.done = true;
</script></body></html>`;

const server = http.createServer((req, res) => {
  const p = decodeURIComponent(req.url.split('?')[0]);
  if (p === '/render.html') { res.writeHead(200, { 'Content-Type': 'text/html' }); return res.end(page); }
  const f = path.join(ROOT, p);
  if (!f.startsWith(ROOT) || !fs.existsSync(f)) { res.writeHead(404); return res.end(); }
  res.writeHead(200, { 'Content-Type': f.endsWith('.js') ? 'application/javascript' : 'text/xml' });
  fs.createReadStream(f).pipe(res);
}).listen(0, async () => {
  const port = server.address().port;
  const browser = await chromium.launch(process.env.CHROMIUM_PATH ? { executablePath: process.env.CHROMIUM_PATH } : {});
  const tab = await browser.newPage({ viewport: { width: W, height: H }, deviceScaleFactor: 2 });
  tab.on('pageerror', e => console.error('pageerror', e.message));
  await tab.goto(`http://127.0.0.1:${port}/render.html`);
  await tab.waitForFunction(() => window.done);
  await tab.waitForTimeout(800);
  await tab.locator('#g').screenshot({ path: outPng });
  const svg = await tab.evaluate(() => {
    const root = window.viewer.graph.getSvg('#ffffff', 1, 0, true, null, true);
    return mxUtils.getXml(root);
  });
  fs.writeFileSync(outSvg, '<?xml version="1.0" encoding="UTF-8"?>\n' + svg);
  await browser.close(); server.close();
  console.log('render ok', W, H);
});

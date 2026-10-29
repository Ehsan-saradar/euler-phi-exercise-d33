// Export PNG (light theme, rendered at 4x by the Viewer) and dual-theme SVG from the
// finalized Archify HTML, by driving the Viewer's own Export menu in headless Chromium.
//
//   node scripts/diagrams-export.mjs <html-dir> <out-dir> <name>...
//
// Needs playwright-core (npm install --no-save playwright-core) and a Chromium build;
// set ARCHIFY_CHROME to a chrome binary to skip Playwright's own browser lookup.
import { chromium } from 'playwright-core';
import { mkdir } from 'node:fs/promises';
import path from 'node:path';

const [inDir, outDir, ...names] = process.argv.slice(2);
if (!inDir || !outDir || names.length === 0) {
  console.error('usage: diagrams-export.mjs <html-dir> <out-dir> <name>...');
  process.exit(2);
}
await mkdir(outDir, { recursive: true });

const browser = await chromium.launch({
  executablePath: process.env.ARCHIFY_CHROME || undefined,
  args: ['--no-sandbox'],
});
const ctx = await browser.newContext({
  colorScheme: 'light',
  viewport: { width: 1600, height: 1000 },
  acceptDownloads: true,
});

for (const name of names) {
  const page = await ctx.newPage();
  await page.goto('file://' + path.resolve(inDir, `${name}.html`), { waitUntil: 'load' });
  await page.waitForSelector('svg', { state: 'attached' });
  await page.waitForTimeout(800); // let fonts and the Viewer runtime settle
  for (const format of ['png', 'svg']) {
    await page.click('[aria-controls="export-menu"]');
    await page.waitForSelector('#export-menu', { state: 'visible' });
    const [download] = await Promise.all([
      page.waitForEvent('download', { timeout: 60000 }),
      page.click(`#export-menu button[data-format="${format}"]`),
    ]);
    await download.saveAs(path.join(outDir, `${name}.${format}`));
    console.log(`${name}.${format}`);
    await page.keyboard.press('Escape');
  }
  await page.close();
}
await browser.close();

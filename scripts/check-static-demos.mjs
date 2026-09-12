import fs from 'node:fs';
import path from 'node:path';
import vm from 'node:vm';
import { fileURLToPath } from 'node:url';

// Dependency-free checks for the GitHub Pages source. Browser checks are still
// required for layout, dynamic links, accessible names, and scenario behavior.
const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const routes = fs.readdirSync(root).filter(name =>
  fs.existsSync(path.join(root, name, 'index.html')) && !name.startsWith('.'));
const directories = ['', 'shared', ...routes];
const failures = [];
let scripts = 0;
let links = 0;

for (const directory of directories) {
  for (const entry of fs.readdirSync(path.join(root, directory), { withFileTypes: true })) {
    if (!entry.isFile()) continue;
    const relative = path.join(directory, entry.name);
    const filename = path.join(root, relative);
    if (entry.name.endsWith('.js')) {
      scripts += 1;
      try { new vm.Script(fs.readFileSync(filename, 'utf8'), { filename: relative }); }
      catch (error) { failures.push(`${relative}: ${error.message}`); }
    }
    if (!entry.name.endsWith('.html')) continue;
    const html = fs.readFileSync(filename, 'utf8');
    for (const match of html.matchAll(/\b(?:href|src)\s*=\s*["']([^"']+)["']/g)) {
      const value = match[1];
      if (/^(?:[a-z]+:|\/\/|#)/i.test(value) || value.includes('{{')) continue;
      const local = decodeURIComponent(value.split(/[?#]/)[0]);
      if (!local) continue;
      links += 1;
      const target = path.resolve(path.dirname(filename), local);
      if (!fs.existsSync(target)) failures.push(`${relative}: missing local link ${value}`);
      else if (fs.statSync(target).isDirectory() && !fs.existsSync(path.join(target, 'index.html'))) {
        failures.push(`${relative}: directory link has no index.html: ${value}`);
      }
    }
  }
}

if (failures.length) {
  console.error(failures.join('\n'));
  process.exitCode = 1;
} else {
  console.log(`PASS: ${routes.length} demo routes, ${scripts} JavaScript files, ${links} local links/assets.`);
}

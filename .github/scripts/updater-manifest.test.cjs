const assert = require('node:assert/strict');
const test = require('node:test');
const { generateManifest } = require('./updater-manifest.cjs');

const packages = {
  'windows-x86_64-msi': 'light-ear_0.3.0_x64_en-US.msi',
  'windows-x86_64-nsis': 'light-ear_0.3.0_x64-setup.exe',
  'linux-x86_64-appimage': 'light-ear_0.3.0_amd64.AppImage',
  'linux-x86_64-deb': 'light-ear_0.3.0_amd64.deb',
  'linux-x86_64-rpm': 'light-ear-0.3.0-1.x86_64.rpm',
  'darwin-aarch64-app': 'light-ear_0.3.0_aarch64.app.tar.gz',
};
const tag = 'v0.3.0';

function draft() {
  const names = Object.values(packages).flatMap(name => [name, name + '.sig']);
  names.push('light-ear-relay-windows.exe', 'latest.json', 'light-ear_0.3.0_aarch64.dmg');
  return {
    draft: true,
    tag_name: tag,
    body: 'Release notes',
    created_at: '2026-01-01T00:00:00Z',
    assets: names.map((name, id) => ({
      name,
      url: `https://api.github.com/repos/example/app/releases/assets/${id}`,
      state: 'uploaded',
      size: 100,
    })),
  };
}

test('builds the nine updater entries from six signed packages, regardless of upload order', () => {
  const release = draft();
  release.assets.reverse();
  const manifest = generateManifest(tag, release, name => `signature for ${name}\n`);
  assert.equal(manifest.version, '0.3.0');
  assert.equal(manifest.notes, release.body);
  assert.equal(manifest.pub_date, release.created_at);
  assert.equal(Object.keys(manifest.platforms).length, 9);
  for (const [target, name] of Object.entries(packages)) {
    const asset = release.assets.find(asset => asset.name === name);
    assert.deepEqual(manifest.platforms[target], { signature: `signature for ${name}.sig`, url: asset.url });
  }
  for (const [os, format] of [['windows-x86_64', 'msi'], ['linux-x86_64', 'appimage'], ['darwin-aarch64', 'app']]) {
    assert.deepEqual(manifest.platforms[os], manifest.platforms[`${os}-${format}`]);
  }
});

test('rejects incomplete uploads, ambiguous packages, and empty signatures', () => {
  const release = draft();
  const [installer, sidecar] = release.assets;
  const generate = () => generateManifest(tag, release, () => 'signature');
  sidecar.state = 'starter';
  assert.throws(generate, /Missing uploaded signature/);
  sidecar.state = 'uploaded';
  assert.throws(() => generateManifest(tag, release, () => '\n'), /Missing signature content/);
  installer.size = 0;
  assert.throws(generate, /found 0/);
  installer.size = 100;
  release.assets.push({ ...installer, name: 'another_x64_en-US.msi' });
  assert.throws(generate, /found 2/);
});

test('requires the draft for the triggering tag', () => {
  const release = draft();
  assert.throws(() => generateManifest('v0.3.1', release), /Expected the release draft/);
  release.draft = false;
  assert.throws(() => generateManifest(tag, release), /Expected the release draft/);
});

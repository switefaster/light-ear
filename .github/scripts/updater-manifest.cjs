const { readFileSync } = require('node:fs');
const { join } = require('node:path');

// Generic targets keep the defaults used in previously published manifests.
const bundles = [
  [/_x64_[^_]+\.msi$/, ['windows-x86_64', 'windows-x86_64-msi']],
  [/_x64-setup\.exe$/, ['windows-x86_64-nsis']],
  [/_amd64\.AppImage$/, ['linux-x86_64', 'linux-x86_64-appimage']],
  [/_amd64\.deb$/, ['linux-x86_64-deb']],
  [/\.x86_64\.rpm$/, ['linux-x86_64-rpm']],
  [/_aarch64\.app\.tar\.gz$/, ['darwin-aarch64', 'darwin-aarch64-app']],
];

function generateManifest(tag, release, readSignature) {
  if (!release.draft || release.tag_name !== tag || !tag.startsWith('v')) {
    throw new Error(`Expected the release draft for ${tag}`);
  }
  const uploaded = release.assets.filter(asset => asset.state === 'uploaded' && asset.size > 0);
  const names = new Set(uploaded.map(asset => asset.name));
  const platforms = {};
  for (const [pattern, targets] of bundles) {
    const matches = uploaded.filter(asset => pattern.test(asset.name));
    if (matches.length !== 1) {
      throw new Error(`Expected one uploaded package for ${targets.at(-1)}, found ${matches.length}`);
    }
    const [asset] = matches;
    const signatureName = `${asset.name}.sig`;
    if (!names.has(signatureName)) {
      throw new Error(`Missing uploaded signature ${signatureName}`);
    }
    const signature = readSignature(signatureName).trim();
    if (!signature || !asset.url) {
      throw new Error(`Missing signature content or download URL for ${asset.name}`);
    }
    for (const target of targets) {
      platforms[target] = { signature, url: asset.url };
    }
  }
  return {
    version: tag.slice(1),
    notes: release.body ?? '',
    pub_date: release.created_at,
    platforms,
  };
}

if (require.main === module) {
  const [tag, releaseFile, signatureDir] = process.argv.slice(2);
  const release = JSON.parse(readFileSync(releaseFile, 'utf8'));
  const manifest = generateManifest(tag, release, name => readFileSync(join(signatureDir, name), 'utf8'));
  console.log(JSON.stringify(manifest, null, 2));
}

module.exports = { generateManifest };

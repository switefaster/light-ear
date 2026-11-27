use super::*;

pub(crate) fn long_mp3() -> tempfile::NamedTempFile {
    let mut file = tempfile::Builder::new().suffix(".mp3").tempfile().unwrap();
    // Two MiB of valid ID3v2.4 padding, followed by ten minutes of 48 kHz
    // MPEG-1 Layer III silence. Alternate 192 and 256 kbit/s every 24 ms.
    file.write_all(b"ID3\x04\0\0\x01\0\0\0").unwrap();
    file.seek(SeekFrom::Current(2 * 1024 * 1024)).unwrap();
    let mut writer = std::io::BufWriter::new(file.as_file_mut());
    for i in 0..25_000 {
        let (header, length) = if i % 2 == 0 { (0xb4, 576) } else { (0xd4, 768) };
        writer.write_all(&[0xff, 0xfb, header, 0]).unwrap();
        writer.write_all(&[0; 764][..length - 4]).unwrap();
    }
    writer.flush().unwrap();
    drop(writer);
    file
}

pub(crate) fn wav(seconds: u32) -> tempfile::NamedTempFile {
    let mut f = tempfile::Builder::new().suffix(".wav").tempfile().unwrap();
    let rate = 8000u32;
    let size = rate * seconds * 2;
    let mut header = b"RIFF".to_vec();
    header.extend((36 + size).to_le_bytes());
    header.extend(b"WAVEfmt ");
    header.extend(16u32.to_le_bytes());
    header.extend(1u16.to_le_bytes());
    header.extend(1u16.to_le_bytes());
    header.extend(rate.to_le_bytes());
    header.extend((rate * 2).to_le_bytes());
    header.extend(2u16.to_le_bytes());
    header.extend(16u16.to_le_bytes());
    header.extend(b"data");
    header.extend(size.to_le_bytes());
    f.write_all(&header).unwrap();
    let block = (0..8000)
        .flat_map(|i| ((i * 17) as i16).to_le_bytes())
        .collect::<Vec<_>>();
    for _ in 0..seconds {
        f.write_all(&block).unwrap()
    }
    f.flush().unwrap();
    f
}
pub(crate) fn scan(f: &tempfile::NamedTempFile) -> (Descriptor, Source) {
    Source::import(
        f.path(),
        "music".into(),
        &identity::Keypair::generate_ed25519(),
        Arc::new(AtomicBool::new(false)),
        |_| {},
    )
    .unwrap()
}
#[test]
fn piece_position_length_room_and_source_are_bound() {
    let mut f = wav(30);
    let (d, mut s) = scan(&f);
    assert_eq!(d.info.duration_ms, 30000);
    assert!(valid_track(&d.track(), "music", Some(&d.info.owner)));
    assert!(!valid_track(&d.track(), "another", None));
    let mut forged = d.clone();
    forged.info.length += 1;
    assert!(!forged.valid("music"));
    for i in 0..d.count() {
        assert!(s.read(&d, i).unwrap().valid(&d, i))
    }
    let mut p = s.read(&d, 2).unwrap();
    assert!(!p.valid(&d, 1));
    p.proof[0][0] ^= 1;
    assert!(!p.valid(&d, 2));
    f.seek(SeekFrom::Start(PIECE as u64)).unwrap();
    f.write_all(b"edited").unwrap();
    f.flush().unwrap();
    assert!(s.read(&d, 1).is_err());
    let mut cache = Store::default();
    let p = s.read(&d, 0).unwrap();
    cache.put(&d, 0, &p).unwrap();
    assert!(cache.read(&d, 0).unwrap().valid(&d, 0));
    cache.retain("music", &HashSet::new());
    assert_eq!(cache.used, 0);
    assert!(cache.read(&d, 0).is_none());
}
#[test]
fn disk_eviction_protects_verified_active_window() {
    let f = wav(30);
    let (d, mut source) = scan(&f);
    let mut cache = Store::default();
    let first = source.read(&d, 0).unwrap();
    let limit = 2 * (first.bytes.len() + first.proof.len() * 32);
    cache.put_with_limit(&d, 0, &first, limit).unwrap();
    cache.pinned = Some((d.key(0), 0));
    for i in 1..3 {
        cache
            .put_with_limit(&d, i, &source.read(&d, i).unwrap(), limit)
            .unwrap()
    }
    assert!(cache.read(&d, 0).is_some());
    assert!(cache.read(&d, 1).is_none());
    assert!(cache.read(&d, 2).is_some());
    assert!(cache.used <= limit);
}
#[test]
fn canceled_import_produces_no_source() {
    let f = wav(4);
    assert!(
        Source::import(
            f.path(),
            "music".into(),
            &identity::Keypair::generate_ed25519(),
            Arc::new(AtomicBool::new(true)),
            |_| {}
        )
        .is_err()
    );
}

//! Simple adaptors to relax the CorTeX conentions for agnostic third-party tooling
use std::error::Error;
use std::fs::{create_dir_all, File};
use std::io::copy;
use std::io::prelude::*;
use std::io::SeekFrom;
use std::io::{Seek, Write};
use std::iter::Iterator;
use std::path::Path;

use tempfile::{tempfile, Builder, TempDir};

use walkdir::{DirEntry, WalkDir};
use zip::write::FileOptions;
use zip::ZipArchive;

/// Transform the ZIP provided by cortex into a TempDir,
/// for e.g. tools such as Engrafo that aren't ZIP-capable
pub fn extract_zip_to_tmpdir(path: &Path, tmpdir_prefix: &str) -> Result<TempDir, Box<dyn Error>> {
    let input_tmpdir = Builder::new().prefix(tmpdir_prefix).tempdir()?;
    let unpacked_dir_path = input_tmpdir.path().to_str().unwrap().to_string() + "/";

    // unpack the Zip file for engrafo
    let inputzip = File::open(path)?;
    let mut input_archive = ZipArchive::new(inputzip)?;
    for i in 0..input_archive.len() {
        let mut file = input_archive.by_index(i)?;
        let extract_path = file.mangled_name();
        let extract_pathname = extract_path.as_path().display();
        let full_pathname = format!("{}{}", unpacked_dir_path, extract_pathname);
        if (file.name()).ends_with('/') {
            create_dir_all(&full_pathname)?;
        } else {
            if let Some(p) = extract_path.parent() {
                if !p.exists() {
                    let absolute_parent = format!("{}{}", unpacked_dir_path, p.display());
                    create_dir_all(absolute_parent)?;
                }
            }
            let mut extracted_file = File::create(&full_pathname)?;
            copy(&mut file, &mut extracted_file)?;
        }
    }
    Ok(input_tmpdir)
}

/// Adaptor that turns an output temporary directory (assuming the filnema conventions are _already_ ollowed)
/// into a ZIP file transmittable back to Cortex
pub fn archive_tmpdir_to_zip(tmpdir: TempDir) -> Result<File, Box<dyn Error>> {
    let dir_path = tmpdir.path().to_str().unwrap();
    archive_directory(dir_path)
}

const METHOD_DEFLATED: zip::CompressionMethod = zip::CompressionMethod::Deflated;

fn archive_directory(src_dir: &str) -> Result<File, Box<dyn Error>> {
    let method = METHOD_DEFLATED;

    let mut file = tempfile()?;

    let walkdir = WalkDir::new(src_dir);
    let it = walkdir.into_iter();

    zip_one_dir(&mut it.filter_map(Result::ok), src_dir, &mut file, method)?;

    file.seek(SeekFrom::Start(0))?;
    Ok(file)
}

fn zip_one_dir<T>(
    it: &mut dyn Iterator<Item = DirEntry>,
    prefix: &str,
    writer: &mut T,
    method: zip::CompressionMethod,
) -> zip::result::ZipResult<()>
where
    T: Write + Seek,
{
    let mut zip = zip::ZipWriter::new(writer);
    let options = FileOptions::default()
        .compression_method(method)
        .unix_permissions(0o755);

    let mut buffer = Vec::new();
    for entry in it {
        let path = entry.path();
        let name = path
            .strip_prefix(Path::new(prefix))
            .unwrap()
            .to_str()
            .unwrap();

        if path.is_file() {
            zip.start_file(name, options)?;
            let mut f = File::open(path)?;

            f.read_to_end(&mut buffer)?;
            zip.write_all(&buffer)?;
            buffer.clear();
        }
    }
    zip.finish()?;
    Result::Ok(())
}

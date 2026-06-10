use rayon::prelude::{IntoParallelIterator, ParallelIterator};
use std::array;
use std::collections::HashMap;
use std::env;
use std::ffi::OsStr;
use std::fs::Metadata;
use std::io;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};

pub mod error;
pub mod filesystem;

use crate::error::{Error, Result};
use crate::filesystem::{self as fs, FileType};

/// Concatenation for the parallel error-accumulating reductions below.
fn concat<T>(mut left: Vec<T>, mut right: Vec<T>) -> Vec<T> {
    left.append(&mut right);
    left
}

// Errors are accumulated and returned rather than written to stderr as they occur: the library
// never prints, so embedders decide how (and whether) to report failures. A failed entry doesn't
// abort the rest of the copy — everything that can be copied is, and all errors come back together.
fn copy_file(source: &Path, source_type: Result<FileType>, dest: &Path) -> Vec<Error> {
    fn __copy_file(source: &Path, source_type: Result<FileType>, dest: &Path) -> Result<Vec<Error>> {
        match source_type? {
            FileType::Regular => {
                fs::copy(source, dest)?;
                fs::copy_timestamps(&fs::symlink_metadata(source)?, dest)?;
            }
            FileType::Directory => return Ok(copy_directory(source, dest)),
            FileType::Symlink => {
                let metadata = fs::symlink_metadata(source)?;
                fs::symlink(fs::read_link(source)?, dest)?;
                fs::copy_timestamps(&metadata, dest)?;
            }
            FileType::Fifo => {
                let metadata = fs::symlink_metadata(source)?;
                fs::mkfifo(dest, metadata.permissions())?;
                fs::copy_timestamps(&metadata, dest)?;
            }
            FileType::Socket => {
                return Err(Error::new(format!(
                    "{}: sockets cannot be copied",
                    source.display(),
                )));
            }
            FileType::CharacterDevice | FileType::BlockDevice => {
                let metadata = fs::symlink_metadata(source)?;
                {
                    let mut source = fs::open(source)?;
                    let mut dest = fs::create(dest, metadata.permissions().mode())?;
                    io::copy(&mut source, &mut dest)?;
                }
                fs::copy_timestamps(&metadata, dest)?;
            }
        }
        Ok(Vec::new())
    }

    __copy_file(source, source_type, dest).unwrap_or_else(|err| vec![err])
}

fn copy_directory(source: &Path, dest: &Path) -> Vec<Error> {
    let metadata = match fs::symlink_metadata(source) {
        Ok(metadata) => metadata,
        Err(err) => return vec![err],
    };
    if let Err(err) = fs::create_dir(dest, metadata.permissions().mode()) {
        return vec![err];
    }
    let dir_entries = match fs::read_dir(source) {
        Ok(dir_entries) => dir_entries,
        Err(err) => return vec![err],
    };
    let (mut entries, mut errors) = (Vec::new(), Vec::new());
    for entry in dir_entries {
        match entry {
            Ok(entry) => entries.push((entry.file_name(), fs::entry_file_type(&entry))),
            Err(err) => errors.push(Error::from(err)),
        }
    }
    entries.shrink_to_fit();
    errors.extend(
        entries
            .into_par_iter()
            .map(|(file_name, file_type)| {
                copy_file(&source.join(&file_name), file_type, &dest.join(&file_name))
            })
            .reduce(Vec::new, concat),
    );
    // Creating entries updates the directory's mtime, so the directory's own
    // timestamps must be copied only after all of its children.
    if let Err(err) = fs::copy_timestamps(&metadata, dest) {
        errors.push(err);
    }
    errors
}

fn reject_self_copies(sources: &[PathBuf], dest: &Path) -> Result<()> {
    let current_dir = env::current_dir()?;
    let mut prefix = Path::new("");
    // We make `dest` absolute because for relative paths the final non-`None` value returned by
    // `Path::ancestors` is always `Some("")`, which is problematic because:
    // 1. It would cause spurious errors, as trying to query the metadata of an empty path
    //    trivially results in an error due to no file corresponding to the given path.
    // 2. In some instances self-copies would not be prevented, as we wouldn't check all of our
    //    ancestors, just the ones up to the current directory.
    let dest = if dest.is_relative() {
        prefix = current_dir.as_path();
        current_dir.join(dest)
    } else {
        dest.to_path_buf()
    };

    // Combine device number with inode number to uniquely identify a file,
    // since the same inode number could be used in a different filesystem.
    fn unique_id(meta: Metadata) -> (u64, u64) {
        (meta.dev(), meta.ino())
    }

    // We use `fs::metadata` for `ancestor_ids` since we do the exact same thing regardless of
    // whether `dest` is a directory or a symlink pointing to one.
    let ancestor_ids = dest
        .ancestors()
        .map(|ancestor| fs::metadata(ancestor).map(unique_id));

    // In contrast, we use `fs::symlink_metadata` for `source_ids` because we copy the symlinks
    // themselves, not the underlying files that they point to.
    let source_ids = sources
        .iter()
        .map(|source| fs::symlink_metadata(source).map(unique_id))
        .collect::<Box<_>>();

    let mut errors = Vec::new();

    for (ancestor, id) in dest.ancestors().zip(ancestor_ids) {
        let id = id?;
        for (source, source_id) in sources.iter().zip(source_ids.as_ref()) {
            match source_id {
                Ok(source_id) if *source_id == id => errors.push(format!(
                    "Cannot copy directory '{}' into itself '{}'",
                    source.display(),
                    ancestor.strip_prefix(prefix).unwrap_or(ancestor).display()
                )),
                Err(err) => errors.push(err.to_string()),
                _ => {}
            }
        }
    }

    if !errors.is_empty() {
        Err(Error::new(errors.join("\n")))
    } else {
        Ok(())
    }
}

fn file_names(sources: &[PathBuf]) -> Result<Vec<&OsStr>> {
    let source_file_names = sources
        .iter()
        .map(|source| {
            source.file_name().ok_or_else(|| {
                Error::new(format!(
                    "{}: path does not end with a file name",
                    source.display()
                ))
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let mut sources_by_name: HashMap<_, Vec<_>> = HashMap::new();
    for (source, file_name) in sources.iter().zip(&source_file_names) {
        sources_by_name.entry(file_name).or_default().push(source);
    }
    let errors = sources_by_name
        .values()
        .filter_map(|source_group| {
                (source_group.len() > 1).then(|| {
                format!(
                "{}: paths have the same file name and thus would be copied to the same destination",
                source_group
                    .iter()
                    .map(|source| format!("{}", source.display()))
                    .collect::<Vec<_>>()
                    .join(", ")
            )
            })
        })
        .collect::<Vec<_>>();
    if !errors.is_empty() {
        Err(Error::new(errors.join("\n")))
    } else {
        Ok(source_file_names)
    }
}

/// Copy each file in `sources` into the directory `dest`.
fn copy_into(sources: &[PathBuf], dest: &Path) -> Vec<Error> {
    if let Some(err) = match fs::metadata(dest) {
        Err(err) => Some(err),
        Ok(metadata) if !metadata.is_dir() => {
            Some(Error::new(format!("{} is not a directory", dest.display())))
        }
        _ => reject_self_copies(sources, dest).err(),
    } {
        return vec![err];
    }

    let file_names = match file_names(sources) {
        Ok(file_names) => file_names,
        Err(err) => return vec![err],
    };
    sources
        .iter()
        .zip(file_names)
        .collect::<Box<_>>()
        .into_par_iter()
        .map(|(source, file_name)| copy_file(source, fs::file_type(source), &dest.join(file_name)))
        .reduce(Vec::new, concat)
}

// The `allow` here is present because clippy doesn't realize that `source` must be of
// type `&PathBuf` in order for the call to `array::from_ref` to typecheck.
#[allow(clippy::ptr_arg)]
fn copy_single(source: &PathBuf, dest: &Path) -> Vec<Error> {
    let source_metadata = match fs::symlink_metadata(source) {
        Ok(metadata) => metadata,
        Err(err) => return vec![err],
    };
    match (fs::metadata(dest), fs::symlink_metadata(dest)) {
        (Ok(metadata), _) if metadata.is_dir() => copy_into(array::from_ref(source), dest),
        (_, Ok(metadata)) if source_metadata.ino() == metadata.ino() => vec![Error::new(format!(
            "Cannot overwrite file '{}' with itself '{}'",
            source.display(),
            dest.display()
        ))],
        _ => copy_file(source, fs::file_type(source), dest),
    }
}

/// Copy each of the leading `args` to the final one, following `cp`'s CLI
/// conventions. Copies as much as possible: a failed entry doesn't abort the
/// remainder, and every failure is reported in the returned error, one per line.
pub fn fcp(args: &[String]) -> Result<()> {
    let args: Box<_> = args.iter().map(PathBuf::from).collect();
    let errors = match args.as_ref() {
        [] | [_] => {
            return Err(Error::new(String::from(
                "Please provide at least two arguments (run 'fcp --help' for details)",
            )));
        }
        [source, dest] => copy_single(source, dest),
        [sources @ .., dest] => copy_into(sources, dest),
    };
    if errors.is_empty() {
        Ok(())
    } else {
        Err(Error::new(
            errors
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join("\n"),
        ))
    }
}

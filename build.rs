use std::fs;
use std::path::{Path, PathBuf};

fn main() {
	let out_dir = std::env::var("OUT_DIR").unwrap();
	let surql_root = if Path::new("crates/oversync-queries/surql").exists() {
		Path::new("crates/oversync-queries/surql")
	} else {
		Path::new("surql")
	};

	// Layer 3a: validate schema + migration .surql files at compile time
	let schema_dir = surql_root.join("schema");
	let migrations_dir = surql_root.join("migrations");
	surql_parser::build::validate_schema(&schema_dir);
	surql_parser::build::validate_schema(&migrations_dir);

	// Layer 3b: generate typed Rust constants for DEFINE FUNCTION in schema
	// Only scans schema/ (not queries/ which contain DML)
	surql_parser::build::generate_typed_functions(
		&schema_dir,
		format!("{out_dir}/surql_functions.rs"),
	);

	generate_embedded_surql(surql_root, Path::new(&out_dir).join("embedded_surql.rs"));
}

fn generate_embedded_surql(surql_root: &Path, out_file: PathBuf) {
	let mut files = Vec::new();
	collect_files(surql_root, surql_root, &mut files);
	files.sort();

	let mut generated = String::from("pub const FILES: &[(&str, &str)] = &[\n");
	for file in files {
		let rel = file.strip_prefix(surql_root).unwrap();
		let rel = rel.to_string_lossy().replace('\\', "/");
		let abs = file.canonicalize().unwrap();
		let abs = abs.to_string_lossy().replace('\\', "\\\\");
		generated.push_str(&format!("\t({rel:?}, include_str!({abs:?})),\n"));
	}
	generated.push_str("];\n");

	fs::write(out_file, generated).unwrap();
}

fn collect_files(root: &Path, dir: &Path, out: &mut Vec<PathBuf>) {
	println!("cargo:rerun-if-changed={}", dir.display());

	let mut entries = fs::read_dir(dir)
		.unwrap()
		.map(|entry| entry.unwrap().path())
		.collect::<Vec<_>>();
	entries.sort();

	for path in entries {
		let rel = path.strip_prefix(root).unwrap().to_string_lossy();
		if rel == ".DS_Store" || rel.ends_with("/.gitkeep") {
			continue;
		}

		if path.is_dir() {
			collect_files(root, &path, out);
		} else {
			out.push(path);
		}
	}
}

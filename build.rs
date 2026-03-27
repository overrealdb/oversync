fn main() {
	let out_dir = std::env::var("OUT_DIR").unwrap();

	// Layer 3a: validate schema + migration .surql files at compile time
	surql_parser::build::validate_schema("crates/oversync-queries/surql/schema/");
	surql_parser::build::validate_schema("crates/oversync-queries/surql/migrations/");

	// Layer 3b: generate typed Rust constants for DEFINE FUNCTION in schema
	// Only scans schema/ (not queries/ which contain DML)
	surql_parser::build::generate_typed_functions(
		"crates/oversync-queries/surql/schema/",
		format!("{out_dir}/surql_functions.rs"),
	);
}

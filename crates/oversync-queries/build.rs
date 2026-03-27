fn main() {
	surql_parser::build::validate_schema("surql/schema/");
	surql_parser::build::validate_schema("surql/migrations/");
}

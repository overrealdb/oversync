use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Row};

use oversync_core::error::OversyncError;

use crate::config::{PipeConfig, PipeRecipeDef, PipeRecipeType, QueryDef, expand_pipe_recipes};

const SYNTHETIC_KEY_COLUMN: &str = "__oversync_key";

#[derive(Debug)]
struct PostgresTableDef {
	schema: String,
	table: String,
	pk_columns: Vec<String>,
}

pub async fn expand_runtime_pipes(
	pipes: Vec<PipeConfig>,
) -> Result<Vec<PipeConfig>, OversyncError> {
	let mut expanded = Vec::with_capacity(pipes.len());
	for pipe in pipes {
		expanded.push(expand_runtime_pipe(pipe).await?);
	}
	Ok(expanded)
}

pub async fn expand_runtime_pipe(pipe: PipeConfig) -> Result<PipeConfig, OversyncError> {
	let mut pipe = expand_pipe_recipes(pipe);

	if !pipe.queries.is_empty() {
		return Ok(pipe);
	}

	let Some(recipe) = pipe.recipe.clone() else {
		return Ok(pipe);
	};

	match recipe.recipe_type {
		PipeRecipeType::PostgresMetadata => Ok(pipe),
		PipeRecipeType::PostgresSnapshot => {
			if pipe.origin.connector != "postgres" {
				return Err(OversyncError::Config(format!(
					"pipe '{}': recipe 'postgres_snapshot' requires origin.connector = 'postgres'",
					pipe.name
				)));
			}

			let queries = expand_postgres_snapshot_queries(&pipe.origin.dsn, &recipe).await?;
			if queries.is_empty() {
				return Err(OversyncError::Config(format!(
					"pipe '{}': recipe 'postgres_snapshot' found no tables with primary keys",
					pipe.name
				)));
			}

			pipe.queries = queries;
			pipe.recipe = None;
			Ok(pipe)
		}
	}
}

async fn expand_postgres_snapshot_queries(
	dsn: &str,
	recipe: &PipeRecipeDef,
) -> Result<Vec<QueryDef>, OversyncError> {
	let pool = PgPoolOptions::new()
		.max_connections(1)
		.connect(dsn)
		.await
		.map_err(|e| OversyncError::Connector(format!("postgres recipe connect: {e}")))?;

	let tables = load_postgres_tables(&pool, &recipe.schemas).await?;
	pool.close().await;

	Ok(tables
		.into_iter()
		.map(|table| postgres_snapshot_query(recipe, table))
		.collect())
}

async fn load_postgres_tables(
	pool: &PgPool,
	schemas: &[String],
) -> Result<Vec<PostgresTableDef>, OversyncError> {
	let rows = if schemas.is_empty() {
		sqlx::query(
			r#"
SELECT
    n.nspname AS schema_name,
    c.relname AS table_name,
    ARRAY_AGG(att.attname ORDER BY array_position(con.conkey, att.attnum)) AS pk_columns
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_namespace n
    ON n.oid = c.relnamespace
JOIN pg_catalog.pg_constraint con
    ON con.conrelid = c.oid
   AND con.contype = 'p'
JOIN pg_catalog.pg_attribute att
    ON att.attrelid = c.oid
   AND att.attnum = ANY(con.conkey)
WHERE c.relkind = 'r'
  AND n.nspname NOT IN ('pg_catalog', 'information_schema')
  AND n.nspname NOT LIKE 'pg_%'
GROUP BY n.nspname, c.relname
ORDER BY n.nspname, c.relname
"#,
		)
		.fetch_all(pool)
		.await
		.map_err(|e| OversyncError::Connector(format!("postgres recipe introspection: {e}")))?
	} else {
		sqlx::query(
			r#"
SELECT
    n.nspname AS schema_name,
    c.relname AS table_name,
    ARRAY_AGG(att.attname ORDER BY array_position(con.conkey, att.attnum)) AS pk_columns
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_namespace n
    ON n.oid = c.relnamespace
JOIN pg_catalog.pg_constraint con
    ON con.conrelid = c.oid
   AND con.contype = 'p'
JOIN pg_catalog.pg_attribute att
    ON att.attrelid = c.oid
   AND att.attnum = ANY(con.conkey)
WHERE c.relkind = 'r'
  AND n.nspname = ANY($1)
GROUP BY n.nspname, c.relname
ORDER BY n.nspname, c.relname
"#,
		)
		.bind(schemas)
		.fetch_all(pool)
		.await
		.map_err(|e| OversyncError::Connector(format!("postgres recipe introspection: {e}")))?
	};

	rows.into_iter()
		.map(|row| {
			let schema = row.try_get::<String, _>("schema_name").map_err(|e| {
				OversyncError::Connector(format!("postgres recipe schema_name: {e}"))
			})?;
			let table = row.try_get::<String, _>("table_name").map_err(|e| {
				OversyncError::Connector(format!("postgres recipe table_name: {e}"))
			})?;
			let pk_columns = row.try_get::<Vec<String>, _>("pk_columns").map_err(|e| {
				OversyncError::Connector(format!("postgres recipe pk_columns: {e}"))
			})?;
			Ok(PostgresTableDef {
				schema,
				table,
				pk_columns,
			})
		})
		.collect()
}

fn postgres_snapshot_query(recipe: &PipeRecipeDef, table: PostgresTableDef) -> QueryDef {
	let key_expr = postgres_key_expr(&table.pk_columns);
	let schema = quote_ident(&table.schema);
	let table_name = quote_ident(&table.table);
	QueryDef {
		id: format!("{}.{}.{}", recipe.prefix, table.schema, table.table),
		sql: format!(
			r#"SELECT
    {key_expr} AS "{SYNTHETIC_KEY_COLUMN}",
    t.*
FROM "{schema}"."{table_name}" t"#,
		),
		key_column: SYNTHETIC_KEY_COLUMN.into(),
		sinks: None,
		transform: None,
	}
}

fn postgres_key_expr(pk_columns: &[String]) -> String {
	if pk_columns.len() == 1 {
		format!(r#"t."{}"::text"#, quote_ident(&pk_columns[0]))
	} else {
		let columns = pk_columns
			.iter()
			.map(|column| format!(r#"t."{}""#, quote_ident(column)))
			.collect::<Vec<_>>()
			.join(", ");
		format!("jsonb_build_array({columns})::text")
	}
}

fn quote_ident(value: &str) -> String {
	value.replace('"', "\"\"")
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::config::{DeltaDef, OriginDef, RetryDef, ScheduleDef};

	fn snapshot_pipe() -> PipeConfig {
		PipeConfig {
			name: "pg".into(),
			origin: OriginDef {
				connector: "postgres".into(),
				dsn: "postgres://example".into(),
				credential: None,
				trino_url: None,
				config: serde_json::Value::Null,
			},
			targets: vec![],
			queries: vec![],
			schedule: ScheduleDef::default(),
			delta: DeltaDef::default(),
			retry: RetryDef::default(),
			recipe: Some(PipeRecipeDef {
				recipe_type: PipeRecipeType::PostgresSnapshot,
				prefix: "postgresdl".into(),
				entity_type_id: None,
				schema_id: "table".into(),
				schemas: vec!["public".into()],
			}),
			filters: vec![],
			transforms: vec![],
			links: vec![],
			alert_webhook: None,
			enabled: true,
		}
	}

	#[test]
	fn postgres_snapshot_query_uses_synthetic_key_for_single_pk() {
		let query = postgres_snapshot_query(
			snapshot_pipe().recipe.as_ref().unwrap(),
			PostgresTableDef {
				schema: "public".into(),
				table: "users".into(),
				pk_columns: vec!["id".into()],
			},
		);

		assert_eq!(query.id, "postgresdl.public.users");
		assert_eq!(query.key_column, SYNTHETIC_KEY_COLUMN);
		assert!(query.sql.contains(r#"t."id"::text AS "__oversync_key""#));
	}

	#[test]
	fn postgres_snapshot_query_uses_json_key_for_composite_pk() {
		let query = postgres_snapshot_query(
			snapshot_pipe().recipe.as_ref().unwrap(),
			PostgresTableDef {
				schema: "public".into(),
				table: "user_roles".into(),
				pk_columns: vec!["user_id".into(), "role_id".into()],
			},
		);

		assert!(query.sql.contains("jsonb_build_array("));
		assert!(query.sql.contains(r#"t."user_id""#));
		assert!(query.sql.contains(r#"t."role_id""#));
	}
}

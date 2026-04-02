mod common;

use serde_json::json;

#[tokio::test]
#[cfg_attr(not(feature = "validate-docker"), ignore = "requires Docker")]
async fn link_rule_crud() {
	let t = common::surreal::TestSurrealContainer::new().await;

	// Create
	t.client
		.query(oversync_queries::links::CREATE_RULE)
		.bind(("name", "email-match"))
		.bind(("rule_type", "exact"))
		.bind((
			"config",
			json!({"left_field": "email", "right_field": "contact_email"}),
		))
		.bind(("enabled", true))
		.await
		.unwrap()
		.check()
		.unwrap();

	// List
	let mut resp = t
		.client
		.query(oversync_queries::links::LIST_RULES)
		.await
		.unwrap()
		.check()
		.unwrap();
	let rules: Vec<serde_json::Value> = resp.take(0).unwrap();
	assert_eq!(rules.len(), 1);
	assert_eq!(rules[0]["name"], "email-match");
	assert_eq!(rules[0]["rule_type"], "exact");
	assert_eq!(rules[0]["enabled"], true);

	// Delete
	t.client
		.query(oversync_queries::links::DELETE_RULE)
		.bind(("name", "email-match"))
		.await
		.unwrap()
		.check()
		.unwrap();

	let mut resp = t
		.client
		.query(oversync_queries::links::LIST_RULES)
		.await
		.unwrap()
		.check()
		.unwrap();
	let rules: Vec<serde_json::Value> = resp.take(0).unwrap();
	assert!(rules.is_empty());
}

#[tokio::test]
#[cfg_attr(not(feature = "validate-docker"), ignore = "requires Docker")]
async fn resolved_link_crud() {
	let t = common::surreal::TestSurrealContainer::new().await;

	// Upsert a link
	t.client
		.query(oversync_queries::links::UPSERT_LINK)
		.bind(("source_key", "user:1"))
		.bind(("target_key", "contact:A"))
		.bind(("rule_name", "email-match"))
		.bind(("confidence", 0.95_f64))
		.await
		.unwrap()
		.check()
		.unwrap();

	// Read
	let mut resp = t
		.client
		.query(oversync_queries::links::READ_LINKS)
		.bind(("source_key", "user:1"))
		.await
		.unwrap()
		.check()
		.unwrap();
	let links: Vec<serde_json::Value> = resp.take(0).unwrap();
	assert_eq!(links.len(), 1);
	assert_eq!(links[0]["source_key"], "user:1");
	assert_eq!(links[0]["target_key"], "contact:A");
	assert_eq!(links[0]["rule_name"], "email-match");
	assert_eq!(links[0]["confidence"], 0.95);

	// Upsert same pair updates confidence
	t.client
		.query(oversync_queries::links::UPSERT_LINK)
		.bind(("source_key", "user:1"))
		.bind(("target_key", "contact:A"))
		.bind(("rule_name", "email-match"))
		.bind(("confidence", 1.0_f64))
		.await
		.unwrap()
		.check()
		.unwrap();

	let mut resp = t
		.client
		.query(oversync_queries::links::READ_LINKS)
		.bind(("source_key", "user:1"))
		.await
		.unwrap()
		.check()
		.unwrap();
	let links: Vec<serde_json::Value> = resp.take(0).unwrap();
	assert_eq!(links.len(), 1);
	assert_eq!(links[0]["confidence"], 1.0);

	// Delete
	t.client
		.query(oversync_queries::links::DELETE_LINK)
		.bind(("source_key", "user:1"))
		.bind(("target_key", "contact:A"))
		.await
		.unwrap()
		.check()
		.unwrap();

	let mut resp = t
		.client
		.query(oversync_queries::links::READ_LINKS)
		.bind(("source_key", "user:1"))
		.await
		.unwrap()
		.check()
		.unwrap();
	let links: Vec<serde_json::Value> = resp.take(0).unwrap();
	assert!(links.is_empty());
}

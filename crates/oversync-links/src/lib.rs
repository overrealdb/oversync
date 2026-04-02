//! Cross-source entity linking for oversync.
//!
//! Match rows across different origins using configurable rules:
//! exact field match, normalized match (lowercase + trim + whitespace),
//! or custom WASM-based matchers (future).

mod rule;

pub use rule::{LinkMatch, LinkRule, MatchStrategy};

/// Find links between rows from different sources.
pub fn find_links(
	left: &[oversync_core::RawRow],
	right: &[oversync_core::RawRow],
	rules: &[LinkRule],
) -> Vec<LinkMatch> {
	let mut matches = Vec::new();
	for l in left {
		for r in right {
			for rule in rules {
				if let Some(confidence) = rule.evaluate(l, r) {
					matches.push(LinkMatch {
						left_key: l.row_key.clone(),
						right_key: r.row_key.clone(),
						rule_name: rule.name.clone(),
						confidence,
					});
				}
			}
		}
	}
	matches
}

#[cfg(test)]
mod tests {
	use super::*;
	use oversync_core::RawRow;
	use serde_json::json;

	fn row(key: &str, data: serde_json::Value) -> RawRow {
		RawRow {
			row_key: key.to_string(),
			row_data: data,
		}
	}

	#[test]
	fn exact_match_finds_identical_fields() {
		let left = vec![row("1", json!({"email": "alice@example.com"}))];
		let right = vec![row("A", json!({"contact_email": "alice@example.com"}))];
		let rules = vec![LinkRule::exact("email-match", "email", "contact_email")];

		let result = find_links(&left, &right, &rules);
		assert_eq!(result.len(), 1);
		assert_eq!(result[0].left_key, "1");
		assert_eq!(result[0].right_key, "A");
		assert_eq!(result[0].confidence, 1.0);
	}

	#[test]
	fn exact_match_no_match_on_different_values() {
		let left = vec![row("1", json!({"email": "alice@example.com"}))];
		let right = vec![row("A", json!({"email": "bob@example.com"}))];
		let rules = vec![LinkRule::exact("email-match", "email", "email")];

		let result = find_links(&left, &right, &rules);
		assert!(result.is_empty());
	}

	#[test]
	fn normalized_match_ignores_case_and_whitespace() {
		let left = vec![row("1", json!({"name": "  Alice  Smith  "}))];
		let right = vec![row("A", json!({"full_name": "alice smith"}))];
		let rules = vec![LinkRule::normalized("name-match", "name", "full_name")];

		let result = find_links(&left, &right, &rules);
		assert_eq!(result.len(), 1);
		assert_eq!(result[0].confidence, 1.0);
	}

	#[test]
	fn normalized_match_no_match_on_different_values() {
		let left = vec![row("1", json!({"name": "Alice"}))];
		let right = vec![row("A", json!({"name": "Bob"}))];
		let rules = vec![LinkRule::normalized("name-match", "name", "name")];

		let result = find_links(&left, &right, &rules);
		assert!(result.is_empty());
	}

	#[test]
	fn null_fields_never_match() {
		let left = vec![row("1", json!({"email": null}))];
		let right = vec![row("A", json!({"email": null}))];
		let rules = vec![LinkRule::exact("email-match", "email", "email")];

		let result = find_links(&left, &right, &rules);
		assert!(result.is_empty());
	}

	#[test]
	fn missing_fields_never_match() {
		let left = vec![row("1", json!({"name": "Alice"}))];
		let right = vec![row("A", json!({"age": 30}))];
		let rules = vec![LinkRule::exact("name-match", "name", "name")];

		let result = find_links(&left, &right, &rules);
		assert!(result.is_empty());
	}

	#[test]
	fn multiple_rules_produce_multiple_matches() {
		let left = vec![row("1", json!({"email": "a@b.com", "phone": "555"}))];
		let right = vec![row("A", json!({"email": "a@b.com", "tel": "555"}))];
		let rules = vec![
			LinkRule::exact("email", "email", "email"),
			LinkRule::exact("phone", "phone", "tel"),
		];

		let result = find_links(&left, &right, &rules);
		assert_eq!(result.len(), 2);
	}

	#[test]
	fn empty_inputs_produce_no_matches() {
		let rules = vec![LinkRule::exact("test", "a", "b")];
		assert!(find_links(&[], &[], &rules).is_empty());
		assert!(find_links(&[row("1", json!({"a": 1}))], &[], &rules).is_empty());
		assert!(find_links(&[], &[row("1", json!({"b": 1}))], &rules).is_empty());
	}

	#[test]
	fn empty_string_exact_match() {
		let left = vec![row("1", json!({"x": ""}))];
		let right = vec![row("A", json!({"x": ""}))];
		let rules = vec![LinkRule::exact("empty", "x", "x")];

		let result = find_links(&left, &right, &rules);
		assert_eq!(result.len(), 1);
	}

	#[test]
	fn unicode_exact_match() {
		let left = vec![row("1", json!({"name": "日本語"}))];
		let right = vec![row("A", json!({"name": "日本語"}))];
		let rules = vec![LinkRule::exact("name", "name", "name")];

		let result = find_links(&left, &right, &rules);
		assert_eq!(result.len(), 1);
	}

	#[test]
	fn unicode_normalized_match() {
		let left = vec![row("1", json!({"name": "  MÜNCHEN  "}))];
		let right = vec![row("A", json!({"name": "münchen"}))];
		let rules = vec![LinkRule::normalized("name", "name", "name")];

		let result = find_links(&left, &right, &rules);
		assert_eq!(result.len(), 1);
	}

	#[test]
	fn link_match_serialization_roundtrip() {
		let m = LinkMatch {
			left_key: "1".into(),
			right_key: "A".into(),
			rule_name: "email".into(),
			confidence: 0.95,
		};
		let json = serde_json::to_string(&m).unwrap();
		let back: LinkMatch = serde_json::from_str(&json).unwrap();
		assert_eq!(m, back);
	}

	#[test]
	fn link_rule_serialization_roundtrip() {
		let rule = LinkRule::exact("email-match", "email", "contact_email");
		let json = serde_json::to_string(&rule).unwrap();
		let back: LinkRule = serde_json::from_str(&json).unwrap();
		assert_eq!(rule.name, back.name);
	}

	#[test]
	fn numeric_values_exact_match() {
		let left = vec![row("1", json!({"code": 42}))];
		let right = vec![row("A", json!({"ref_code": 42}))];
		let rules = vec![LinkRule::exact("code", "code", "ref_code")];

		let result = find_links(&left, &right, &rules);
		assert_eq!(result.len(), 1);
	}

	#[test]
	fn normalized_match_trims_inner_whitespace() {
		let left = vec![row("1", json!({"addr": "123   Main    St"}))];
		let right = vec![row("A", json!({"address": "123 main st"}))];
		let rules = vec![LinkRule::normalized("addr", "addr", "address")];

		let result = find_links(&left, &right, &rules);
		assert_eq!(result.len(), 1);
	}
}

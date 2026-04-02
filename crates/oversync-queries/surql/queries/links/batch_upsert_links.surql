FOR $link IN $links {
    UPSERT type::record('resolved_link', [$link.source_key, $link.target_key]) SET
        source_key = $link.source_key,
        target_key = $link.target_key,
        rule_name = $link.rule_name,
        confidence = $link.confidence,
        created_at = time::now();
}

LET $now = time::now();
LET $expiry = $now + type::duration(type::string($ttl_secs) + "s");
LET $existing = (SELECT * FROM pipe_lock WHERE pipe = $pipe LIMIT 1);
LET $existing_rows = IF $existing = NONE {
    []
} ELSE IF type::is_array($existing) {
    $existing
} ELSE {
    [$existing]
};
IF array::len($existing_rows) = 0 OR $existing_rows[0].expires_at < $now OR $existing_rows[0].instance = $instance {
    DELETE pipe_lock WHERE pipe = $pipe;
    CREATE pipe_lock SET pipe = $pipe, instance = $instance, acquired_at = $now, expires_at = $expiry;
    RETURN { acquired: true };
} ELSE {
    RETURN { acquired: false };
};

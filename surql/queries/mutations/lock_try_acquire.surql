LET $now = time::now();
LET $expiry = $now + type::duration(type::string($ttl_secs) + "s");
LET $existing = (SELECT * FROM pipe_lock WHERE pipe = $pipe LIMIT 1);
IF array::len($existing) = 0 OR $existing[0].expires_at < $now OR $existing[0].instance = $instance {
    DELETE pipe_lock WHERE pipe = $pipe;
    CREATE pipe_lock SET pipe = $pipe, instance = $instance, acquired_at = $now, expires_at = $expiry;
    RETURN { acquired: true };
} ELSE {
    RETURN { acquired: false };
};

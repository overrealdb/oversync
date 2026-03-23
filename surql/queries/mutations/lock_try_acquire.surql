LET $now = time::now();
LET $existing = (SELECT * FROM pipe_lock WHERE pipe = $pipe LIMIT 1);
IF array::len($existing) = 0 OR $existing[0].expires_at < $now OR $existing[0].instance = $instance {
    UPSERT pipe_lock SET pipe = $pipe, instance = $instance, acquired_at = $now, expires_at = $now + type::duration(type::string($ttl_secs) + "s") WHERE pipe = $pipe;
    RETURN { acquired: true };
} ELSE {
    RETURN { acquired: false };
};

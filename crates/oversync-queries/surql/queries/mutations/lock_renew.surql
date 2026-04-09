LET $now = time::now();
LET $expiry = $now + type::duration(type::string($ttl_secs) + "s");
LET $updated = (
    UPDATE pipe_lock
    SET expires_at = $expiry
    WHERE pipe = $pipe AND instance = $instance
);
RETURN { renewed: array::len($updated) > 0 };

LET $now = time::now();
LET $expiry = $now + type::duration(type::string($ttl_secs) + "s");
LET $updated = (
    UPDATE pipe_lock
    SET expires_at = $expiry
    WHERE pipe = $pipe AND instance = $instance
);
LET $updated_rows = IF $updated = NONE {
    []
} ELSE IF type::is_array($updated) {
    $updated
} ELSE {
    [$updated]
};
RETURN { renewed: array::len($updated_rows) > 0 };

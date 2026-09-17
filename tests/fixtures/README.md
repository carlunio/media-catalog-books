# Fixture de migración v0.1.1

`books-v0.1.1.duckdb.gz` se generó ejecutando `scripts/init_db.py` del tag
local `v0.1.1` con DuckDB 1.4.4. Después se insertó un libro de muestra y sus
registros relacionados mediante el esquema público de esa versión, se ejecutó
`CHECKPOINT` y se comprimió con gzip.

El fixture no contiene datos reales ni secretos. Su SHA-256 comprimido es:

```text
b500e0bb9d4d2133e11b92be2c203754459a2a368454e3c557a363fc3d8e6fff
```

Su finalidad es demostrar que las migraciones actuales conservan tablas,
campos, datos editados manualmente y la vista exportada de `v0.1.1`.

# avro_turf

## v0.8.0
- The names `AvroTurf::SchemaRegistry`, `AvroTurf::CachedSchemaRegistry`, and
  `FakeSchemaRegistryServer` are deprecated and will be removed in a future release.
  Use `AvroTurf::ConfluentSchemaRegistry`, `AvroTurf::CachedConfluentSchemaRegistry`,
  and `FakeConfluentSchemaRegistryServer` instead.
- Add support for the Config API (http://docs.confluent.io/3.1.2/schema-registry/docs/api.html#config)
  to `AvroTurf::ConfluentSchemaRegistry`.

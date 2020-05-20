# AvroTurf

## Unreleased

## v1.0.0

- Stop caching nested sub-schemas (#111)

## v0.11.0

- Add proxy support (#107)
- Adding support for client certs (#109)

## v0.10.0

- Add more disk caching (#103)
- Include schema information when decoding (#100, #101, #104)

## v0.9.0

- Compatibility with Avro v1.9.0 (#94)
- Disable the auto registeration of schema (#95)
- abstracted caching from CachedConfluentSchemaRegistry (#74)
- Load avro-patches if installed to silence deprecation errors (#85)
- Make schema store to be thread safe (#92)

## v0.8.1

- Allow accessing schema store from outside AvroTurf (#68).

## v0.8.0

- The names `AvroTurf::SchemaRegistry`, `AvroTurf::CachedSchemaRegistry`, and
  `FakeSchemaRegistryServer` are deprecated and will be removed in a future release.
  Use `AvroTurf::ConfluentSchemaRegistry`, `AvroTurf::CachedConfluentSchemaRegistry`,
  and `FakeConfluentSchemaRegistryServer` instead.
- Add support for the Config API (http://docs.confluent.io/3.1.2/schema-registry/docs/api.html#config)
  to `AvroTurf::ConfluentSchemaRegistry`.

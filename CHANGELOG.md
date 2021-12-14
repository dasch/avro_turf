# AvroTurf

## Unreleased

## v1.5.0

- Add CA cert file option (#157)
- Add compatibility with Avro v1.11.x.

## v1.4.1

- Purge sub-schemas from cache before re-parsing schema (#151)

## v1.4.0

- Add support for Ruby 3 (#146)
- Add ability to validate message before encoding in `AvroTurf#encode` interface

## v1.3.1

- Prevent CachedConfluentSchemaRegistry from caching the 'latest' version (#140)
- Fix issue with zero length schema cache file (#138)

## v1.3.0

- Add support for plain user/password auth to ConfluentSchemaRegistry (#120)

## v1.2.0

- Expose `fetch_schema`, `fetch_schema_by_id` and `register_schema` schema in `Messaging` interface (#117, #119)
- Add ability to validate message before encoding in `Messaging#encode` interface (#116, #118)

## v1.1.0

- Compatibility with Avro v1.10.x.

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

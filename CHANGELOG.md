# AvroTurf

## Unreleased

## v1.17.0

- Add `register_schemas` option to `encode` method [#210](https://github.com/dasch/avro_turf/pull/210)

## v1.16.0

- Add compatibility with Avro v1.12.x.

## v1.15.0

- Use `default_namespace` from exception to load nested schemas from the correct namespace. (#203)
- Bump minimum avro version to 1.11.3
- Add support for schema contexts (#205)

## v1.14.0

- Add `resolv_resolver` parameter to `AvroTurf::Messaging` to make use of custom domain name resolvers and their options, for example `nameserver` and `timeouts` (#202)
- Stop using `Excon`'s `dns_timeouts` in favour of `resolv_resolver` because `dns_timeouts` is now deprecated due to https://github.com/excon/excon/issues/832 (#202)

## v1.13.0

- Set `idempotent: true` for the request except one that registers a new schema (#199)
- Use `connect_timeout` for `Excon`'s `dns_timeouts` that set the timeout for the connection to the Domain Name Server (#201)

## v1.12.0

- Add `connect_timeout` parameter to `AvroTurf::Messaging` to set the timeout for the connection to the schema registry (#197)

## v1.11.0

- Add `decode_all` and `decode_all_from_stream` methods to return all entries in a data file (#194)
- Improve the way schemas are automatically loaded (#190)
- Increment dependency on `avro` gem to v1.8.

## v1.10.0

- Add `schema_subject_versions` to `ConfluentSchemaRegistry` to retrieve all subject versions for a schema id. (#189)
- `FakeConfluentSchemaRegistryServer` now returns same id if identical schema is created for a different subject (#188)

## v1.9.0

- Send Accept and User-Agent headers on every request (#184)

## v1.8.0

- Add support for `Date` via appropriate logicalType defintion.  This is a backwards incompatible change  (#177)
- Fixed schema file cache truncation on multiple running instances and parallel access to the cache files.

## v1.7.0

- Added extra params for the validation message schema before encode (#169)
- Fix infinite retry when loading schema with nested primary type in separate file (#165)

## v1.6.0

- Schema registry path prefix (#162)

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
- Disable the auto registration of schema (#95)
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

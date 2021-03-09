# AvroTurf

AvroTurf is a library that makes it easier to encode and decode data using the [Apache Avro](http://avro.apache.org/) serialization format. It adds a layer on top of the official Avro gem which makes it easier to integrate Avro into your application:

* Provides an idiomatic Ruby interface.
* Allows referencing schemas defined in another file.

## Deprecation Notice

The `AvroTurf::SchemaRegistry`, `AvroTurf::CachedSchemaRegistry`,
and `FakeSchemaRegistryServer` names have been deprecated because the Avro spec recently
introduced an incompatible [single-message encoding format](https://github.com/apache/avro/commit/30408a9c192c5f4eaaf42f01f0ffbfffd705aa57).

These classes have been renamed to `AvroTurf::ConfluentSchemaRegistry`,
`AvroTurf::CachedConfluentSchemaRegistry`, and `FakeConfluentSchemaRegistry`.

The aliases for the original names will be removed in a future release.

## Note about finding nested schemas

As of AvroTurf version 1.0.0, only top-level schemas that have their own .avsc file will be loaded and resolvable by the `AvroTurf::SchemaStore#find` method. This change will likely not affect most users. However, if you use `AvroTurf::SchemaStore#load_schemas!` to pre-cache all your schemas and then rely on `AvroTurf::SchemaStore#find` to access nested schemas that are not defined by their own .avsc files, your code may stop working when you upgrade to v1.0.0.

As an example, if you have a `person` schema (defined in `my/schemas/contacts/person.avsc`) that defines a nested `address` schema like this:

```json
{
  "name": "person",
  "namespace": "contacts",
  "type": "record",
  "fields": [
    {
      "name": "address",
      "type": {
        "name": "address",
        "type": "record",
        "fields": [
          { "name": "addr1", "type": "string" },
          { "name": "addr2", "type": "string" },
          { "name": "city", "type": "string" },
          { "name": "zip", "type": "string" }
        ]
      }
    }
  ]
}
```
...this will no longer work in v1.0.0:
```ruby
store = AvroTurf::SchemaStore.new(path: 'my/schemas')
store.load_schemas!

# Accessing 'person' is correct and works fine.
person = store.find('person', 'contacts') # my/schemas/contacts/person.avsc exists

# Trying to access 'address' raises AvroTurf::SchemaNotFoundError
address = store.find('address', 'contacts') # my/schemas/contacts/address.avsc is not found
```

For details and context, see [this pull request](https://github.com/dasch/avro_turf/pull/111).

## Installation

Add this line to your application's Gemfile:

```ruby
gem 'avro_turf'
```

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install avro_turf

## Usage

Using AvroTurf is quite simple:

```ruby
# Schemas will be looked up from the specified directory.
avro = AvroTurf.new(schemas_path: "app/schemas/")

# Decode some data using a named schema. The schema file should exist in the
# schemas directory with the file name `<name>.avsc`.
avro.decode(encoded_data, schema_name: "person")

# Encode some data using the named schema.
avro.encode({ "name" => "Jane", "age" => 28 }, schema_name: "person")

# Data can be validated before encoding to get a description of problem through
# Avro::SchemaValidator::ValidationError exception
avro.encode({ "titl" => "hello, world" }, schema_name: "person", validate: true)
```

### Inter-schema references

Unlike the official Avro library, AvroTurf allows schemas to reference each other. As an example:

```json
// person.avsc
{
  "name": "person",
  "type": "record",
  "fields": [
    {
      "name": "full_name",
      "type": "string"
    },
    {
      "name": "address",
      "type": "address"
    }
  ]
}

// address.avsc
{
  "name": "address",
  "type": "record",
  "fields": [
    {
      "name": "street",
      "type": "string"
    },
    {
      "name": "city",
      "type": "string"
    }
  ]
}
```

In the example above, the `person` schema references the `address` schema, even though the latter is defined in another file. This makes it possible to share types across schemas, e.g.

```json
// person_list.avsc
{
  "type": "array",
  "items": "person"
}
```

There's no reason to copy-paste the `person` schema into the `person_list` schema, as you can reference it directly.

This feature helps avoid subtle errors when the same type is represented using slightly different schemas.


### Using a Schema Registry

By default, AvroTurf will encode data in the Avro data file format. This means that the schema used to encode the data is prepended to the output. If you want to decrease the size of the output, e.g. when storing data in a log such as Apache Kafka or in a database, you can use the `AvroTurf::Messaging` API. This top-level API requires the use of [Schema Registry](https://github.com/confluentinc/schema-registry), a service which allows registering and fetching Avro schemas.

The Messaging API will automatically register schemas used for encoding data, and will fetch the corresponding schema when decoding. Instead of including the full schema in the output, only a schema id generated by the registry is included. Registering the same schema twice is idempotent, so no coordination is needed.

**NOTE:** [The Messaging format](https://github.com/confluentinc/schema-registry/blob/master/docs/serializer-formatter.rst#wire-format) is _not_ compatible with the Avro data file API.

The Messaging API is not included by default, so you must require 'avro_turf/messaging' explicitly if you want to use it.

Using the Messaging API is simple once you have set up a Schema Registry service:

```ruby
require 'avro_turf/messaging'

# You need to pass the URL of your Schema Registry.
avro = AvroTurf::Messaging.new(registry_url: "http://my-registry:8081/")

# The API for encoding and decoding data is similar to the default one. Encoding
# data has the side effect of registering the schema. This only happens the first
# time a schema is used.
data = avro.encode({ "title" => "hello, world" }, schema_name: "greeting")

# If you don't want to automatically register new schemas, you can pass explicitly
# subject and version to specify which schema should be used for encoding.
# It will fetch that schema from the registry and cache it. Subsequent instances
# of the same schema version will be served by the cache.
data = avro.encode({ "title" => "hello, world" }, subject: 'greeting', version: 1)

# You can also pass explicitly schema_id to specify which schema
# should be used for encoding.
# It will fetch that schema from the registry and cache it. Subsequent instances
# of the same schema version will be served by the cache.
data = avro.encode({ "title" => "hello, world" }, schema_id: 2)

# Message can be validated before encoding to get a description of problem through
# Avro::SchemaValidator::ValidationError exception
data = avro.encode({ "titl" => "hello, world" }, schema_name: "greeting", validate: true)

# When decoding, the schema will be fetched from the registry and cached. Subsequent
# instances of the same schema id will be served by the cache.
avro.decode(data) #=> { "title" => "hello, world" }

# If you want to get decoded message as well as the schema used to encode the message,
# you can use `#decode_message` method.
result = avro.decode_message(data)
result.message       #=> { "title" => "hello, world" }
result.schema_id     #=> 3
result.writer_schema #=> #<Avro::Schema: ...>
result.reader_schema #=> nil

# You can also work with schema through this interface:
# Fetch latest schema for subject from registry
schema, schema_id = avro.fetch_schema(subject: 'greeting')
# Fetch specific version
schema, schema_id = avro.fetch_schema(subject: 'greeting', version: 1)
# Fetch schema by id
schema, schema_id = avro.fetch_schema_by_id(3)
# Register schema fetched from store by name
schema, schema_id = avro.register_schema(schema_name: 'greeting')
# Specify namespace (same as schema_name: 'somewhere.greeting')
schema, schema_id = avro.register_schema(schema_name: 'greeting', namespace: 'somewhere')
# Customize subject under which to register schema
schema, schema_id = avro.register_schema(schema_name: 'greeting', namespace: 'somewhere', subject: 'test')
```

### Confluent Schema Registry Client

The ConfluentSchemaRegistry client used by the Messaging API can also be used directly.
It can check whether a schema is compatible with a subject in the registry using the [Compatibility API](http://docs.confluent.io/3.1.2/schema-registry/docs/api.html#compatibility):

```ruby
require 'avro_turf'
require 'avro_turf/confluent_schema_registry'

schema = <<-JSON
{
  "name": "person",
  "type": "record",
  "fields": [
    {
      "name": "full_name",
      "type": "string"
    },
    {
      "name": "address",
      "type": "address"
    }
  ]
}
JSON

registry = AvroTurf::ConfluentSchemaRegistry.new("http://my-registry:8081/")

# Returns true if the schema is compatible, nil if the subject or version is not registered, and false if incompatible.
registry.compatible?("person", schema)
```

The ConfluentSchemaRegistry client can also change the global compatibility level or the compatibility level for an individual subject using the [Config API](http://docs.confluent.io/3.1.2/schema-registry/docs/api.html#config):

```ruby
registry.update_global_config(compatibility: 'FULL')
registry.update_subject_config("person", compatibility: 'NONE')
```

### Testing Support

AvroTurf includes a `FakeConfluentSchemaRegistryServer` that can be used in tests. The
fake schema registry server depends on Sinatra but it is _not_ listed as a runtime
dependency for AvroTurf. Sinatra must be added to your Gemfile or gemspec in order
to use the fake server.

Example using RSpec:

```ruby
require 'avro_turf/test/fake_confluent_schema_registry_server'
require 'webmock/rspec'

# within an example
let(:registry_url) { "http://registry.example.com" }
before do
  stub_request(:any, /^#{registry_url}/).to_rack(FakeConfluentSchemaRegistryServer)
  FakeConfluentSchemaRegistryServer.clear
end

# Messaging objects created with the same registry_url will now use the fake server.
```

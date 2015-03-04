# AvroTurf

AvroTurf is a library that makes it easier to encode and decode data using the [Apache Avro](http://avro.apache.org/) serialization format. It adds a layer on top of the official Avro gem which makes it easier to integrate Avro into your application:

* Provides an idiomatic Ruby interface.
* Allows referencing schemas defined in another file.

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
  "name": "person_list",
  "type": "array",
  "items": "person"
}
```

There's no reason to copy-paste the `person` schema into the `person_list` schema, as you can reference it directly.

This feature helps avoid subtle errors when the same type is represented using slightly different schemas.

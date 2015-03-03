# AvroTurf

AvroTurf is a library that makes it easier to encode and decode data using the [Apache Avro](http://avro.apache.org/) serialization format.

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

```ruby
# Schemas will be looked up from the specified directory.
avro = AvroTurf.new(schemas_path: "app/schemas/")

# Decode some data using a named schema. The schema file should exist in the
# schemas directory with the file name `<name>.avsc`.
avro.decode(encoded_data, schema_name: "person")

# Encode some data using the named schema.
avro.encode({ "name" => "Jane", "age" => 28 }, schema_name: "person")
```

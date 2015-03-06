require 'avro_turf/version'
require 'avro'

class AvroTurf
  class Error < StandardError; end
  class SchemaError < Error; end

  def initialize(schemas_path:)
    @schemas_path = schemas_path or raise "Please specify a schema path"
  end

  # Encodes data to Avro using the specified schema.
  #
  # data        - The data that should be encoded.
  # schema_name - The name of a schema in the `schemas_path`.
  #
  # Returns a String containing the encoded data.
  def encode(data, schema_name:)
    schema = resolve_schema(schema_name)
    writer = Avro::IO::DatumWriter.new(schema)

    io = StringIO.new
    dw = Avro::DataFile::Writer.new(io, writer, schema)
    dw << data
    dw.close

    io.string
  end

  # Decodes Avro data.
  #
  # encoded_data - A String containing Avro-encoded data.
  # schema_name  - The String name of the schema that should be used to read
  #                the data. If nil, the writer schema will be used.
  #
  # Returns whatever is encoded in the data.
  def decode(encoded_data, schema_name: nil)
    io = StringIO.new(encoded_data)
    schema = schema_name && resolve_schema(schema_name)
    reader = Avro::IO::DatumReader.new(nil, schema)
    dr = Avro::DataFile::Reader.new(io, reader)
    dr.first
  end

  private

  # Resolves and returns a schema.
  #
  # schema_name - The String name of the schema to resolve.
  # names       - A Hash mapping schema names to Avro::Schema instances. Used
  #               when referencing custom types.
  #
  # Returns an Avro::Schema.
  def resolve_schema(fullname, names = {})
    *namespace, schema_name = fullname.split(".")
    schema_path = File.join(@schemas_path, *namespace, schema_name + ".avsc")
    schema_json = JSON.parse(File.read(schema_path))
    schema = Avro::Schema.real_parse(schema_json, names)

    if schema.respond_to?(:fullname) && schema.fullname != fullname
      raise SchemaError, "expected schema `#{schema_path}' to define type `#{fullname}'"
    end

    schema
  rescue ::Avro::SchemaParseError => e
    # This is a hack in order to figure out exactly which type was missing. The
    # Avro gem ought to provide this data directly.
    if e.to_s =~ /"([\w\.]+)" is not a schema we know about/
      resolve_schema($1, names)

      # Re-resolve the original schema now that the dependency has been resolved.
      names.delete(fullname)
      resolve_schema(fullname, names)
    else
      raise
    end
  end
end

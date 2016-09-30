require 'webmock/rspec'

# This spec verifies the monkey-patch that we have to apply until the avro
# gem releases a fix for bug AVRO-1848:
# https://issues.apache.org/jira/browse/AVRO-1848

describe Avro::Schema do
  it "correctly handles falsey field defaults" do
    schema = Avro::Schema.parse <<-SCHEMA
      {"type": "record", "name": "Record", "namespace": "my.name.space",
        "fields": [
          {"name": "is_usable", "type": "boolean", "default": false}
        ]
      }
    SCHEMA
    
    expect(schema.to_avro).to eq({
      'type' => 'record', 'name' => 'Record', 'namespace' => 'my.name.space',
      'fields' => [
        {'name' => 'is_usable', 'type' => 'boolean', 'default' => false}
      ]
    })
  end
end


describe Avro::IO::DatumReader do
  let(:writer_schema) do
    Avro::Schema.parse <<-AVSC
      {
        "name": "no_default",
        "type": "record",
        "fields": [
          { "type": "string", "name": "one" }
        ]
      }
    AVSC
  end
  let(:reader_schema) do
    Avro::Schema.parse <<-AVSC
      {
        "name": "no_default",
        "type": "record",
        "fields": [
          { "type": "string", "name": "one" },
          { "type": "string", "name": "two" }
        ]
      }
    AVSC
  end

  it "raises an error for missing fields without a default" do
    stream = StringIO.new
    writer = Avro::IO::DatumWriter.new(writer_schema)
    encoder = Avro::IO::BinaryEncoder.new(stream)
    writer.write({ 'one' => 'first' }, encoder)
    encoded = stream.string

    stream = StringIO.new(encoded)
    decoder = Avro::IO::BinaryDecoder.new(stream)
    reader = Avro::IO::DatumReader.new(writer_schema, reader_schema)
    expect do
      reader.read(decoder)
    end.to raise_error(Avro::AvroError, 'Missing data for "string" with no default')
  end
end

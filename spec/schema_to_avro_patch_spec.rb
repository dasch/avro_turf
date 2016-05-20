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

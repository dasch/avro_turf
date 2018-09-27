require 'rack/test'
require 'avro_turf/test/fake_confluent_schema_registry_server'

describe FakeConfluentSchemaRegistryServer do
  include Rack::Test::Methods

  def app; described_class; end

  let(:schema) do
    {
      type: "record",
      name: "person",
      fields: [
        { name: "name", type: "string" }
      ]
    }.to_json
  end

  describe 'POST /subjects/:subject/versions' do
    it 'returns the same schema ID when invoked with same schema and same subject' do
      post '/subjects/person/versions', { schema: schema }.to_json, 'CONTENT_TYPE' => 'application/vnd.schemaregistry+json'

      expected_id = JSON.parse(last_response.body).fetch('id')

      post '/subjects/person/versions', { schema: schema }.to_json, 'CONTENT_TYPE' => 'application/vnd.schemaregistry+json'

      expect(JSON.parse(last_response.body).fetch('id')).to eq expected_id
    end

    it 'returns a different schema ID when invoked with same schema and different subject' do
      post '/subjects/person/versions', { schema: schema }.to_json, 'CONTENT_TYPE' => 'application/vnd.schemaregistry+json'

      original_id = JSON.parse(last_response.body).fetch('id')

      post '/subjects/happy-person/versions', { schema: schema }.to_json, 'CONTENT_TYPE' => 'application/vnd.schemaregistry+json'

      expect(JSON.parse(last_response.body).fetch('id')).not_to eq original_id
    end
  end
end

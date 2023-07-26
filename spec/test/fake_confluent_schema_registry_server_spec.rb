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

    it 'returns the same schema ID when invoked with same schema and different subject' do
      post '/subjects/person/versions', { schema: schema }.to_json, 'CONTENT_TYPE' => 'application/vnd.schemaregistry+json'

      original_id = JSON.parse(last_response.body).fetch('id')

      post '/subjects/happy-person/versions', { schema: schema }.to_json, 'CONTENT_TYPE' => 'application/vnd.schemaregistry+json'

      expect(JSON.parse(last_response.body).fetch('id')).to eq original_id
    end

    it 'returns a different schema ID when invoked with a different schema' do
      post '/subjects/person/versions', { schema: schema }.to_json, 'CONTENT_TYPE' => 'application/vnd.schemaregistry+json'

      original_id = JSON.parse(last_response.body).fetch('id')

      other_schema = {
        type: "record",
        name: "other",
        fields: [
          { name: "name", type: "string" }
        ]
      }.to_json

      post '/subjects/person/versions', { schema: other_schema }.to_json, 'CONTENT_TYPE' => 'application/vnd.schemaregistry+json'

      expect(JSON.parse(last_response.body).fetch('id')).to_not eq original_id
    end
  end

  describe 'GET /schemas/ids/:id/versions' do
    def schema(name:)
      {
        type: "record",
        name: name,
        fields: [
          { name: "name", type: "string" },
        ]
      }.to_json
    end

    it "returns array containing subjects and versions for given schema id" do
      schema1 = schema(name: "name1")
      schema2 = schema(name: "name2")

      post "/subjects/cats/versions", { schema: schema1 }.to_json, 'CONTENT_TYPE' => 'application/vnd.schemaregistry+json'
      schema1_id = JSON.parse(last_response.body).fetch('id') # Original cats schema

      post "/subjects/dogs/versions", { schema: schema2 }.to_json, 'CONTENT_TYPE' => 'application/vnd.schemaregistry+json'
      post "/subjects/cats/versions", { schema: schema2 }.to_json, 'CONTENT_TYPE' => 'application/vnd.schemaregistry+json'
      schema2_id = JSON.parse(last_response.body).fetch('id') # Changed cats schema == Original Dogs schema

      get "/schemas/ids/#{schema1_id}/versions"
      result = JSON.parse(last_response.body)

      expect(result).to eq [{
        'subject' => 'cats',
        'version' => 1
      }]

      get "/schemas/ids/#{schema2_id}/versions"
      result = JSON.parse(last_response.body)

      expect(result).to include( {
        'subject' => 'cats',
        'version' => 2
      }, {
        'subject' => 'dogs',
        'version' => 1
      })
    end
  end
end

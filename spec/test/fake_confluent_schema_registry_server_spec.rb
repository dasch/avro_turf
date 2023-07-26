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

  describe 'GET /schemas/ids/:id/versions' do
    let(:other_schema) do
      {
        type: "record",
        name: "person",
        fields: [
          { name: "name", type: "string" },
          { name: "age", type: "int" }
        ]
      }.to_json
    end

    it "returns array with one element containing subject and version for given schema id" do
      subject = 'customer'
      post "/subjects/#{subject}/versions", { schema: schema }.to_json, 'CONTENT_TYPE' => 'application/vnd.schemaregistry+json'
      post "/subjects/#{subject}/versions", { schema: other_schema }.to_json, 'CONTENT_TYPE' => 'application/vnd.schemaregistry+json'
      schema_id = JSON.parse(last_response.body).fetch('id')

      get "/schemas/ids/#{schema_id}/versions"

      result = JSON.parse(last_response.body)

      expect(result).to eq [{
        'subject' => subject,
        'version' => 2
      }]
    end
  end
end

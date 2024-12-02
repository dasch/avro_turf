require 'rack/test'

describe FakeConfluentSchemaRegistryServer do
  include Rack::Test::Methods

  def app; AuthorizedFakeConfluentSchemaRegistryServer; end

  describe 'POST /subjects/:subject/versions' do
    it 'returns the same schema ID when invoked with same schema and same subject' do
      post '/subjects/person/versions', { schema: schema(name: "person") }.to_json, 'CONTENT_TYPE' => 'application/vnd.schemaregistry+json'

      expected_id = JSON.parse(last_response.body).fetch('id')

      post '/subjects/person/versions', { schema: schema(name: "person") }.to_json, 'CONTENT_TYPE' => 'application/vnd.schemaregistry+json'

      expect(JSON.parse(last_response.body).fetch('id')).to eq expected_id
    end

    it 'returns the same schema ID when invoked with same schema and different subject' do
      post '/subjects/person/versions', { schema: schema(name: "person") }.to_json, 'CONTENT_TYPE' => 'application/vnd.schemaregistry+json'

      original_id = JSON.parse(last_response.body).fetch('id')

      post '/subjects/happy-person/versions', { schema: schema(name: "person") }.to_json, 'CONTENT_TYPE' => 'application/vnd.schemaregistry+json'

      expect(JSON.parse(last_response.body).fetch('id')).to eq original_id
    end

    it 'returns a different schema ID when invoked with a different schema' do
      post '/subjects/person/versions', { schema: schema(name: "person") }.to_json, 'CONTENT_TYPE' => 'application/vnd.schemaregistry+json'

      original_id = JSON.parse(last_response.body).fetch('id')

      post '/subjects/person/versions', { schema: schema(name: "other") }.to_json, 'CONTENT_TYPE' => 'application/vnd.schemaregistry+json'

      expect(JSON.parse(last_response.body).fetch('id')).to_not eq original_id
    end

    context 'with a clean registry' do
      before do
        FakeConfluentSchemaRegistryServer.clear
      end

      it 'assigns same schema id for different schemas in different contexts' do
        post '/subjects/:.context1:cats/versions', { schema: schema(name: 'name1') }.to_json, 'CONTENT_TYPE' => 'application/vnd.schemaregistry+json'
        schema1_id = JSON.parse(last_response.body).fetch('id') # Original cats schema

        post '/subjects/:.context2:dogs/versions', { schema: schema(name: 'name2') }.to_json, 'CONTENT_TYPE' => 'application/vnd.schemaregistry+json'
        schema2_id = JSON.parse(last_response.body).fetch('id') # Original cats schema

        expect(schema1_id).to eq(schema2_id)
      end
    end
  end

  describe 'GET /schemas/ids/:id/versions' do

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

    describe 'schema registry contexts' do
      it 'allows different schemas to have same schema version', :aggregate_failures do
        petSchema = schema(name: 'pet_cat')
        animalSchema = schema(name: 'animal_cat')

        post '/subjects/:.pets:cats/versions', { schema: petSchema }.to_json, 'CONTENT_TYPE' => 'application/vnd.schemaregistry+json'
        pet_id = JSON.parse(last_response.body).fetch('id') # Context1 cats schema

        post '/subjects/:.animals:cats/versions', { schema: animalSchema }.to_json, 'CONTENT_TYPE' => 'application/vnd.schemaregistry+json'
        animal_id = JSON.parse(last_response.body).fetch('id') # Context2 cats schema

        get "/schemas/ids/#{pet_id}/versions?subject=:.pets:"
        result = JSON.parse(last_response.body)

        expect(result).to eq [{
          'subject' => ':.pets:cats',
          'version' => 1
        }]

        get "/schemas/ids/#{animal_id}/versions?subject=:.animals:"
        result = JSON.parse(last_response.body)

        expect(result).to eq [{
          'subject' => ':.animals:cats',
          'version' => 1
        }]
      end
    end
  end

  describe 'GET /schemas/ids/:schema_id' do
    it 'returns schema by id', :aggregate_failures do
      petSchema = schema(name: 'pet_ferret')

      post '/subjects/ferret/versions', { schema: petSchema }.to_json, 'CONTENT_TYPE' => 'application/vnd.schemaregistry+json'
      pet_id = JSON.parse(last_response.body).fetch('id')

      get "/schemas/ids/#{pet_id}"
      result = JSON.parse(last_response.body)

      expect(result['schema']).to eq(petSchema)

      get "/schemas/ids/#{pet_id}?subject=:.:"
      default_context_result = JSON.parse(last_response.body)

      expect(default_context_result['schema']).to eq(petSchema)
    end

    it 'returns schema by id from a non default context' do
      petSchema = schema(name: 'pet_ferret_too')

      post '/subjects/:.pets:ferret/versions', { schema: petSchema }.to_json, 'CONTENT_TYPE' => 'application/vnd.schemaregistry+json'
      pet_id = JSON.parse(last_response.body).fetch('id')

      get "/schemas/ids/#{pet_id}?subject=:.pets:"
      result = JSON.parse(last_response.body)

      expect(result['schema']).to eq(petSchema)
    end
  end

  describe 'GET /subjects' do
    context 'with a clean registry' do
      before do
        FakeConfluentSchemaRegistryServer.clear
      end

      it "returns subjects from all contexts", :aggregate_failures do
        post '/subjects/ferret/versions', { schema: schema(name: 'ferret') }.to_json, 'CONTENT_TYPE' => 'application/vnd.schemaregistry+json'
        post '/subjects/:.pets:cat/versions', { schema: schema(name: 'pet_cat') }.to_json, 'CONTENT_TYPE' => 'application/vnd.schemaregistry+json'

        get "/subjects"
        result = JSON.parse(last_response.body)

        expect(result).to include('ferret')
        expect(result).to include(':.pets:cat')
      end
    end
  end

  describe 'GET /subjects/:subject/versions' do
    it 'returns versions of the schema' do
      post '/subjects/gerbil/versions', { schema: schema(name: 'v1') }.to_json, 'CONTENT_TYPE' => 'application/vnd.schemaregistry+json'
      post '/subjects/gerbil/versions', { schema: schema(name: 'v2') }.to_json, 'CONTENT_TYPE' => 'application/vnd.schemaregistry+json'

      get "/subjects/gerbil/versions"
      result = JSON.parse(last_response.body)

      expect(result).to include(1)
      expect(result).to include(2)
    end

    it 'returns does not see versions ion another context' do
      post '/subjects/gerbil/versions', { schema: schema(name: 'v1') }.to_json, 'CONTENT_TYPE' => 'application/vnd.schemaregistry+json'
      post '/subjects/:.test:gerbil/versions', { schema: schema(name: 'v2') }.to_json, 'CONTENT_TYPE' => 'application/vnd.schemaregistry+json'

      get "/subjects/:.test:gerbil/versions"
      result = JSON.parse(last_response.body)

      expect(result).to include(1)
    end
  end

  describe 'GET /subjects/:subject/versions/:version', :aggregate_failures do
    it 'returns the schema by version' do
      schema1 = schema(name: 'v1')
      post '/subjects/gerbil/versions', { schema: schema1}.to_json, 'CONTENT_TYPE' => 'application/vnd.schemaregistry+json'
      id1 = JSON.parse(last_response.body).fetch('id')

      schema2 = schema(name: 'v2')
      post '/subjects/gerbil/versions', { schema: schema2}.to_json, 'CONTENT_TYPE' => 'application/vnd.schemaregistry+json'
      id2 = JSON.parse(last_response.body).fetch('id')

      get '/subjects/gerbil/versions/1'
      result = JSON.parse(last_response.body)
      expect(result['subject']).to eq('gerbil')
      expect(result['version']).to eq(1)
      expect(result['id']).to eq(id1)
      expect(result['schema']).to eq(schema1)

      get '/subjects/gerbil/versions/2'
      result = JSON.parse(last_response.body)
      expect(result['subject']).to eq('gerbil')
      expect(result['version']).to eq(2)
      expect(result['id']).to eq(id2)
      expect(result['schema']).to eq(schema2)
    end
  end

  def schema(name:)
    {
      type: "record",
      name: name,
      fields: [
        { name: "name", type: "string" },
      ]
    }.to_json
  end
end

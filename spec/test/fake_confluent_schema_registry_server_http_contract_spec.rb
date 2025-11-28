# frozen_string_literal: true

# HTTP Contract Tests for FakeConfluentSchemaRegistryServer
#
# These tests verify the exact HTTP behavior of the fake schema registry server,
# including status codes, headers, and response body structure.
# They serve as a specification for the Rack-based replacement of Sinatra.

require "rack/test"

RSpec.describe "FakeConfluentSchemaRegistryServer HTTP Contract" do
  include Rack::Test::Methods

  def app
    AuthorizedFakeConfluentSchemaRegistryServer
  end

  before do
    # Must call clear on the actual app class to reset the global_config class instance variable
    AuthorizedFakeConfluentSchemaRegistryServer.clear
  end

  def schema(name: "test_schema")
    {
      type: "record",
      name: name,
      fields: [
        {name: "name", type: "string"}
      ]
    }.to_json
  end

  def json_content_type
    "application/vnd.schemaregistry+json"
  end

  def post_json(path, body)
    post path, body.to_json, "CONTENT_TYPE" => json_content_type
  end

  def put_json(path, body)
    put path, body.to_json, "CONTENT_TYPE" => json_content_type
  end

  describe "Response Headers" do
    # Note: Sinatra defaults to text/html, we'll preserve this behavior
    # but our Rack replacement could potentially improve this
    it "returns a content type for successful requests" do
      post_json "/subjects/test/versions", {schema: schema}

      # Sinatra defaults to text/html;charset=utf-8
      expect(last_response.content_type).to be_truthy
    end

    it "returns a content type for error responses" do
      get "/schemas/ids/999"

      expect(last_response.content_type).to be_truthy
    end
  end

  describe "POST /subjects/:subject/versions" do
    it "returns 200 status for successful registration" do
      post_json "/subjects/test-subject/versions", {schema: schema}

      expect(last_response.status).to eq(200)
    end

    it "returns JSON with 'id' key" do
      post_json "/subjects/test-subject/versions", {schema: schema}

      body = JSON.parse(last_response.body)
      expect(body).to have_key("id")
      expect(body["id"]).to be_a(Integer)
    end

    it "returns same id for same schema in same subject" do
      post_json "/subjects/test-subject/versions", {schema: schema}
      first_id = JSON.parse(last_response.body)["id"]

      post_json "/subjects/test-subject/versions", {schema: schema}
      second_id = JSON.parse(last_response.body)["id"]

      expect(second_id).to eq(first_id)
    end

    it "returns same id for same schema in different subject" do
      post_json "/subjects/subject1/versions", {schema: schema}
      first_id = JSON.parse(last_response.body)["id"]

      post_json "/subjects/subject2/versions", {schema: schema}
      second_id = JSON.parse(last_response.body)["id"]

      expect(second_id).to eq(first_id)
    end

    it "returns different id for different schema" do
      post_json "/subjects/test-subject/versions", {schema: schema(name: "schema1")}
      first_id = JSON.parse(last_response.body)["id"]

      post_json "/subjects/test-subject/versions", {schema: schema(name: "schema2")}
      second_id = JSON.parse(last_response.body)["id"]

      expect(second_id).not_to eq(first_id)
    end

    context "with schema context" do
      it "supports qualified subject names" do
        post_json "/subjects/:.context1:test/versions", {schema: schema}

        expect(last_response.status).to eq(200)
        body = JSON.parse(last_response.body)
        expect(body).to have_key("id")
      end

      it "isolates schemas by context" do
        post_json "/subjects/:.ctx1:test/versions", {schema: schema(name: "s1")}
        id1 = JSON.parse(last_response.body)["id"]

        post_json "/subjects/:.ctx2:test/versions", {schema: schema(name: "s2")}
        id2 = JSON.parse(last_response.body)["id"]

        # Different contexts start from 0
        expect(id1).to eq(id2)
      end
    end
  end

  describe "GET /schemas/ids/:schema_id" do
    it "returns 200 status for existing schema" do
      post_json "/subjects/test/versions", {schema: schema}
      schema_id = JSON.parse(last_response.body)["id"]

      get "/schemas/ids/#{schema_id}"

      expect(last_response.status).to eq(200)
    end

    it "returns JSON with 'schema' key containing the schema JSON" do
      test_schema = schema(name: "my_schema")
      post_json "/subjects/test/versions", {schema: test_schema}
      schema_id = JSON.parse(last_response.body)["id"]

      get "/schemas/ids/#{schema_id}"

      body = JSON.parse(last_response.body)
      expect(body).to have_key("schema")
      expect(body["schema"]).to eq(test_schema)
    end

    it "returns 404 status for non-existent schema" do
      get "/schemas/ids/999"

      expect(last_response.status).to eq(404)
    end

    it "returns error JSON for non-existent schema" do
      get "/schemas/ids/999"

      body = JSON.parse(last_response.body)
      expect(body["error_code"]).to eq(40403)
      expect(body["message"]).to eq("Schema not found")
    end

    context "with schema context" do
      it "fetches schema from specified context via query param" do
        test_schema = schema(name: "ctx_schema")
        post_json "/subjects/:.myctx:test/versions", {schema: test_schema}
        schema_id = JSON.parse(last_response.body)["id"]

        get "/schemas/ids/#{schema_id}?subject=:.myctx:"

        expect(last_response.status).to eq(200)
        body = JSON.parse(last_response.body)
        expect(body["schema"]).to eq(test_schema)
      end
    end
  end

  describe "GET /schemas/ids/:schema_id/versions" do
    it "returns 200 status for existing schema" do
      post_json "/subjects/test/versions", {schema: schema}
      schema_id = JSON.parse(last_response.body)["id"]

      get "/schemas/ids/#{schema_id}/versions"

      expect(last_response.status).to eq(200)
    end

    it "returns array of subject/version objects" do
      post_json "/subjects/test-subject/versions", {schema: schema}
      schema_id = JSON.parse(last_response.body)["id"]

      get "/schemas/ids/#{schema_id}/versions"

      body = JSON.parse(last_response.body)
      expect(body).to be_an(Array)
      expect(body.first).to have_key("subject")
      expect(body.first).to have_key("version")
      expect(body.first["subject"]).to eq("test-subject")
      expect(body.first["version"]).to eq(1)
    end

    it "returns all subjects using the schema" do
      test_schema = schema(name: "shared")
      post_json "/subjects/subject1/versions", {schema: test_schema}
      post_json "/subjects/subject2/versions", {schema: test_schema}
      schema_id = JSON.parse(last_response.body)["id"]

      get "/schemas/ids/#{schema_id}/versions"

      body = JSON.parse(last_response.body)
      subjects = body.map { |v| v["subject"] }
      expect(subjects).to include("subject1", "subject2")
    end

    it "returns 404 for non-existent schema" do
      get "/schemas/ids/999/versions"

      expect(last_response.status).to eq(404)
    end
  end

  describe "GET /subjects" do
    it "returns 200 status" do
      get "/subjects"

      expect(last_response.status).to eq(200)
    end

    it "returns empty array when no subjects" do
      get "/subjects"

      body = JSON.parse(last_response.body)
      expect(body).to eq([])
    end

    it "returns array of subject names" do
      post_json "/subjects/subject1/versions", {schema: schema(name: "s1")}
      post_json "/subjects/subject2/versions", {schema: schema(name: "s2")}

      get "/subjects"

      body = JSON.parse(last_response.body)
      expect(body).to include("subject1", "subject2")
    end

    it "includes subjects from all contexts" do
      post_json "/subjects/plain-subject/versions", {schema: schema(name: "s1")}
      post_json "/subjects/:.ctx:context-subject/versions", {schema: schema(name: "s2")}

      get "/subjects"

      body = JSON.parse(last_response.body)
      expect(body).to include("plain-subject")
      expect(body).to include(":.ctx:context-subject")
    end
  end

  describe "GET /subjects/:subject/versions" do
    it "returns 200 status for existing subject" do
      post_json "/subjects/test/versions", {schema: schema}

      get "/subjects/test/versions"

      expect(last_response.status).to eq(200)
    end

    it "returns array of version numbers" do
      post_json "/subjects/test/versions", {schema: schema(name: "v1")}
      post_json "/subjects/test/versions", {schema: schema(name: "v2")}

      get "/subjects/test/versions"

      body = JSON.parse(last_response.body)
      expect(body).to eq([1, 2])
    end

    it "returns 404 for non-existent subject" do
      get "/subjects/nonexistent/versions"

      expect(last_response.status).to eq(404)
    end

    it "returns error JSON for non-existent subject" do
      get "/subjects/nonexistent/versions"

      body = JSON.parse(last_response.body)
      expect(body["error_code"]).to eq(40401)
      expect(body["message"]).to eq("Subject not found")
    end
  end

  describe "GET /subjects/:subject/versions/:version" do
    before do
      post_json "/subjects/test/versions", {schema: schema(name: "version1")}
      @schema1 = schema(name: "version1")
      @id1 = JSON.parse(last_response.body)["id"]

      post_json "/subjects/test/versions", {schema: schema(name: "version2")}
      @schema2 = schema(name: "version2")
      @id2 = JSON.parse(last_response.body)["id"]
    end

    it "returns 200 status for existing version" do
      get "/subjects/test/versions/1"

      expect(last_response.status).to eq(200)
    end

    it "returns full schema details" do
      get "/subjects/test/versions/1"

      body = JSON.parse(last_response.body)
      expect(body["subject"]).to eq("test")
      expect(body["version"]).to eq(1)
      expect(body["id"]).to eq(@id1)
      expect(body["schema"]).to eq(@schema1)
    end

    it "returns correct version when version number specified" do
      get "/subjects/test/versions/2"

      body = JSON.parse(last_response.body)
      expect(body["version"]).to eq(2)
      expect(body["id"]).to eq(@id2)
      expect(body["schema"]).to eq(@schema2)
    end

    it "supports 'latest' as version" do
      get "/subjects/test/versions/latest"

      body = JSON.parse(last_response.body)
      expect(body["version"]).to eq(2)
      expect(body["schema"]).to eq(@schema2)
    end

    it "returns 404 for non-existent subject" do
      get "/subjects/nonexistent/versions/1"

      expect(last_response.status).to eq(404)
      body = JSON.parse(last_response.body)
      expect(body["error_code"]).to eq(40401)
    end

    it "returns 404 for non-existent version" do
      get "/subjects/test/versions/99"

      expect(last_response.status).to eq(404)
      body = JSON.parse(last_response.body)
      expect(body["error_code"]).to eq(40402)
      expect(body["message"]).to eq("Version not found")
    end
  end

  describe "POST /subjects/:subject (check schema)" do
    before do
      @test_schema = schema(name: "registered")
      post_json "/subjects/test/versions", {schema: @test_schema}
      @schema_id = JSON.parse(last_response.body)["id"]
    end

    it "returns 200 status for registered schema" do
      post_json "/subjects/test", {schema: @test_schema}

      expect(last_response.status).to eq(200)
    end

    it "returns schema details for registered schema" do
      post_json "/subjects/test", {schema: @test_schema}

      body = JSON.parse(last_response.body)
      expect(body["subject"]).to eq("test")
      expect(body["id"]).to eq(@schema_id)
      expect(body["version"]).to eq(1)
      expect(body["schema"]).to eq(@test_schema)
    end

    it "returns 404 for unregistered schema" do
      post_json "/subjects/test", {schema: schema(name: "unregistered")}

      expect(last_response.status).to eq(404)
      body = JSON.parse(last_response.body)
      expect(body["error_code"]).to eq(40403)
    end
  end

  describe "GET /config" do
    it "returns 200 status" do
      get "/config"

      expect(last_response.status).to eq(200)
    end

    it "returns default global config" do
      get "/config"

      body = JSON.parse(last_response.body)
      expect(body["compatibility"]).to eq("BACKWARD")
    end
  end

  describe "PUT /config" do
    it "returns 200 status" do
      put_json "/config", {compatibility: "FULL"}

      expect(last_response.status).to eq(200)
    end

    it "updates and returns the new config" do
      put_json "/config", {compatibility: "FULL"}

      body = JSON.parse(last_response.body)
      expect(body["compatibility"]).to eq("FULL")
    end

    it "persists the updated config" do
      put_json "/config", {compatibility: "NONE"}

      get "/config"
      body = JSON.parse(last_response.body)
      expect(body["compatibility"]).to eq("NONE")
    end
  end

  describe "GET /config/:subject" do
    it "returns 200 status" do
      get "/config/test-subject"

      expect(last_response.status).to eq(200)
    end

    it "returns global config when subject config not set" do
      get "/config/test-subject"

      body = JSON.parse(last_response.body)
      expect(body["compatibility"]).to eq("BACKWARD")
    end

    it "returns subject-specific config when set" do
      put_json "/config/test-subject", {compatibility: "FORWARD"}

      get "/config/test-subject"

      body = JSON.parse(last_response.body)
      expect(body["compatibility"]).to eq("FORWARD")
    end
  end

  describe "PUT /config/:subject" do
    it "returns 200 status" do
      put_json "/config/test-subject", {compatibility: "FORWARD"}

      expect(last_response.status).to eq(200)
    end

    it "updates and returns the subject config" do
      put_json "/config/test-subject", {compatibility: "FORWARD"}

      body = JSON.parse(last_response.body)
      expect(body["compatibility"]).to eq("FORWARD")
    end

    it "does not affect global config" do
      put_json "/config/test-subject", {compatibility: "NONE"}

      get "/config"
      body = JSON.parse(last_response.body)
      expect(body["compatibility"]).to eq("BACKWARD")
    end

    it "does not affect other subjects" do
      put_json "/config/subject1", {compatibility: "NONE"}

      get "/config/subject2"
      body = JSON.parse(last_response.body)
      expect(body["compatibility"]).to eq("BACKWARD")
    end
  end

  describe "clear class method" do
    it "resets all state" do
      post_json "/subjects/test/versions", {schema: schema}
      put_json "/config", {compatibility: "NONE"}

      # Must call clear on the same class used as app
      AuthorizedFakeConfluentSchemaRegistryServer.clear

      get "/subjects"
      expect(JSON.parse(last_response.body)).to eq([])

      get "/config"
      expect(JSON.parse(last_response.body)["compatibility"]).to eq("BACKWARD")
    end
  end
end

RSpec.describe "FakePrefixedConfluentSchemaRegistryServer HTTP Contract" do
  include Rack::Test::Methods

  def app
    AuthorizedFakePrefixedConfluentSchemaRegistryServer
  end

  before do
    # Must call clear on the actual app class to reset the global_config class instance variable
    AuthorizedFakePrefixedConfluentSchemaRegistryServer.clear
  end

  def schema(name: "test_schema")
    {
      type: "record",
      name: name,
      fields: [
        {name: "name", type: "string"}
      ]
    }.to_json
  end

  def json_content_type
    "application/vnd.schemaregistry+json"
  end

  def post_json(path, body)
    post path, body.to_json, "CONTENT_TYPE" => json_content_type
  end

  def put_json(path, body)
    put path, body.to_json, "CONTENT_TYPE" => json_content_type
  end

  describe "prefixed routes" do
    describe "POST /prefix/subjects/:subject/versions" do
      it "returns 200 status and schema id" do
        post_json "/prefix/subjects/test/versions", {schema: schema}

        expect(last_response.status).to eq(200)
        body = JSON.parse(last_response.body)
        expect(body).to have_key("id")
      end
    end

    describe "GET /prefix/schemas/ids/:schema_id" do
      it "returns schema by id" do
        post_json "/prefix/subjects/test/versions", {schema: schema}
        schema_id = JSON.parse(last_response.body)["id"]

        get "/prefix/schemas/ids/#{schema_id}"

        expect(last_response.status).to eq(200)
        body = JSON.parse(last_response.body)
        expect(body).to have_key("schema")
      end

      it "returns 404 for non-existent schema" do
        get "/prefix/schemas/ids/999"

        expect(last_response.status).to eq(404)
      end
    end

    describe "GET /prefix/subjects" do
      it "returns list of subjects" do
        post_json "/prefix/subjects/test1/versions", {schema: schema(name: "s1")}
        post_json "/prefix/subjects/test2/versions", {schema: schema(name: "s2")}

        get "/prefix/subjects"

        expect(last_response.status).to eq(200)
        body = JSON.parse(last_response.body)
        expect(body).to include("test1", "test2")
      end
    end

    describe "GET /prefix/subjects/:subject/versions" do
      it "returns version list" do
        post_json "/prefix/subjects/test/versions", {schema: schema(name: "v1")}
        post_json "/prefix/subjects/test/versions", {schema: schema(name: "v2")}

        get "/prefix/subjects/test/versions"

        expect(last_response.status).to eq(200)
        body = JSON.parse(last_response.body)
        expect(body).to eq([1, 2])
      end

      it "returns 404 for non-existent subject" do
        get "/prefix/subjects/nonexistent/versions"

        expect(last_response.status).to eq(404)
      end
    end

    describe "GET /prefix/subjects/:subject/versions/:version" do
      it "returns schema details" do
        test_schema = schema(name: "versioned")
        post_json "/prefix/subjects/test/versions", {schema: test_schema}
        schema_id = JSON.parse(last_response.body)["id"]

        get "/prefix/subjects/test/versions/1"

        expect(last_response.status).to eq(200)
        body = JSON.parse(last_response.body)
        expect(body["name"]).to eq("test")
        expect(body["version"]).to eq(1)
        expect(body["id"]).to eq(schema_id)
        expect(body["schema"]).to eq(test_schema)
      end

      it "supports 'latest' version" do
        post_json "/prefix/subjects/test/versions", {schema: schema(name: "v1")}
        post_json "/prefix/subjects/test/versions", {schema: schema(name: "v2")}

        get "/prefix/subjects/test/versions/latest"

        expect(last_response.status).to eq(200)
        body = JSON.parse(last_response.body)
        expect(body["version"]).to eq(2)
      end
    end

    describe "POST /prefix/subjects/:subject (check schema)" do
      it "returns schema details for registered schema" do
        test_schema = schema(name: "check")
        post_json "/prefix/subjects/test/versions", {schema: test_schema}
        schema_id = JSON.parse(last_response.body)["id"]

        post_json "/prefix/subjects/test", {schema: test_schema}

        expect(last_response.status).to eq(200)
        body = JSON.parse(last_response.body)
        expect(body["subject"]).to eq("test")
        expect(body["id"]).to eq(schema_id)
      end

      it "returns 404 for unregistered schema" do
        post_json "/prefix/subjects/test/versions", {schema: schema(name: "one")}

        post_json "/prefix/subjects/test", {schema: schema(name: "other")}

        expect(last_response.status).to eq(404)
      end
    end

    describe "GET /prefix/config" do
      it "returns global config" do
        get "/prefix/config"

        expect(last_response.status).to eq(200)
        body = JSON.parse(last_response.body)
        expect(body["compatibility"]).to eq("BACKWARD")
      end
    end

    describe "PUT /prefix/config" do
      it "updates global config" do
        put_json "/prefix/config", {compatibility: "FULL"}

        expect(last_response.status).to eq(200)
        body = JSON.parse(last_response.body)
        expect(body["compatibility"]).to eq("FULL")
      end
    end

    describe "GET /prefix/config/:subject" do
      it "returns subject config or global default" do
        get "/prefix/config/test-subject"

        expect(last_response.status).to eq(200)
        body = JSON.parse(last_response.body)
        expect(body["compatibility"]).to eq("BACKWARD")
      end
    end

    describe "PUT /prefix/config/:subject" do
      it "updates subject config" do
        put_json "/prefix/config/test-subject", {compatibility: "NONE"}

        expect(last_response.status).to eq(200)
        body = JSON.parse(last_response.body)
        expect(body["compatibility"]).to eq("NONE")
      end
    end
  end
end

RSpec.describe "Host Authorization" do
  include Rack::Test::Methods

  def schema
    {
      type: "record",
      name: "test",
      fields: [{name: "name", type: "string"}]
    }.to_json
  end

  describe "AuthorizedFakeConfluentSchemaRegistryServer" do
    def app
      AuthorizedFakeConfluentSchemaRegistryServer
    end

    it "allows requests from permitted hosts" do
      # The default Rack::Test host is "example.org" which is in the permitted list
      get "/subjects"

      expect(last_response.status).to eq(200)
    end
  end

  describe "AuthorizedFakePrefixedConfluentSchemaRegistryServer" do
    def app
      AuthorizedFakePrefixedConfluentSchemaRegistryServer
    end

    it "allows requests from permitted hosts" do
      get "/prefix/subjects"

      expect(last_response.status).to eq(200)
    end
  end
end

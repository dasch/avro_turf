require 'excon'

class AvroTurf::ConfluentSchemaRegistry
  CONTENT_TYPE = "application/vnd.schemaregistry.v1+json".freeze

  def initialize(url)
    @connection = Excon.new(url, headers: { "Content-Type" => CONTENT_TYPE })
  end

  def fetch(id)
    AvroTurf.logger.info "Fetching schema with id #{id}"
    data = get("/schemas/ids/#{id}")
    data.fetch("schema")
  end

  def register(subject, schema)
    data = post("/subjects/#{subject}/versions", body: {
      schema: schema.to_s
    }.to_json)

    id = data.fetch("id")

    AvroTurf.logger.info "Registered schema for subject `#{subject}`; id = #{id}"

    id
  end

  # List all subjects
  def subjects
    get('/subjects')
  end

  # List all versions for a subject
  def subject_versions(subject)
    get("/subjects/#{subject}/versions")
  end

  # Get a specific version for a subject
  def subject_version(subject, version = 'latest')
    get("/subjects/#{subject}/versions/#{version}")
  end

  # Check if a schema exists. Returns nil if not found.
  def check(subject, schema)
    data = post("/subjects/#{subject}",
                expects: [200, 404],
                body: { schema: schema.to_s }.to_json)
    data unless data.has_key?("error_code")
  end

  # Check if a schema is compatible with the stored version.
  # Returns:
  # - true if compatible
  # - nil if the subject or version does not exist
  # - false if incompatible
  # http://docs.confluent.io/3.1.2/schema-registry/docs/api.html#compatibility
  def compatible?(subject, schema, version = 'latest')
    data = post("/compatibility/subjects/#{subject}/versions/#{version}",
                expects: [200, 404],
                body: { schema: schema.to_s }.to_json)
    data.fetch('is_compatible', false) unless data.has_key?('error_code')
  end

  # Get global config
  def global_config
    get("/config")
  end

  # Update global config
  def update_global_config(config)
    put("/config", { body: config.to_json })
  end

  # Get config for subject
  def subject_config(subject)
    get("/config/#{subject}")
  end

  # Update config for subject
  def update_subject_config(subject, config)
    put("/config/#{subject}", { body: config.to_json })
  end

  private

  def get(path, **options)
    request(path, method: :get, **options)
  end

  def put(path, **options)
    request(path, method: :put, **options)
  end

  def post(path, **options)
    request(path, method: :post, **options)
  end

  def request(path, **options)
    options = { expects: 200 }.merge!(options)
    response = @connection.request(path: path, **options)
    JSON.parse(response.body)
  end
end

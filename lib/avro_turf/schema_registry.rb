require 'excon'

class AvroTurf::SchemaRegistry
  CONTENT_TYPE = "application/vnd.schemaregistry.v1+json".freeze

  def initialize(url, logger: Logger.new($stdout))
    @logger = logger
    @connection = Excon.new(url, headers: {
      "Content-Type" => CONTENT_TYPE,
    })
  end

  def fetch(id)
    @logger.info "Fetching schema with id #{id}"
    data = get("/schemas/ids/#{id}")
    data.fetch("schema")
  end

  def register(subject, schema)
    data = post("/subjects/#{subject}/versions", body: {
      schema: schema.to_s
    }.to_json)

    id = data.fetch("id")

    @logger.info "Registered schema for subject `#{subject}`; id = #{id}"

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

  private

  def get(path, **options)
    request(path, method: :get, **options)
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

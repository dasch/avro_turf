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
      schema: schema
    }.to_json)

    id = data.fetch("id")

    @logger.info "Registered schema for subject `#{subject}`; id = #{id}"

    id
  end

  private

  def get(path, **options)
    request(path, method: :get, **options)
  end

  def post(path, **options)
    request(path, method: :post, **options)
  end

  def request(path, **options)
    response = @connection.request(path: path, expects: 200, **options)
    JSON.parse(response.body)
  end
end

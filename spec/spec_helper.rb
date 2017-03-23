require 'bundler/setup'
require 'json_spec'
require 'fakefs/spec_helpers'
require 'avro_turf'

Dir["#{File.dirname(__FILE__)}/support/**/*.rb"].each { |f| require f }

module Helpers
  def define_schema(path, content)
    File.open(File.join("spec/schemas", path), "w") do |f|
      f.write(content)
    end
  end
end

RSpec.configure do |config|
  config.include FakeFS::SpecHelpers
  config.include Helpers
end

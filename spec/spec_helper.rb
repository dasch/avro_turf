# frozen_string_literal: true

require "bundler/setup"
require "logger"
require "json_spec"
require "pp" # Require pp before fakefs to fix TypeError: superclass mismatch for class File
require "fakefs/spec_helpers"
require "avro_turf"

Dir["#{File.dirname(__FILE__)}/support/**/*.rb"].each { |f| require f }

module Helpers
  def define_schema(path, content)
    file = File.join("spec/schemas", path)
    dir = File.dirname(file)
    FileUtils.mkdir_p(dir)
    File.write(file, content)
  end

  def store_cache(path, hash)
    File.write(File.join("spec/cache", path), JSON.generate(hash))
  end

  def load_cache(path)
    JSON.parse(File.read(File.join("spec/cache", path)))
  end
end

# gem `fakefs` does not support flock for the file, and require patch
# https://github.com/fakefs/fakefs/issues/433
module FakeFS
  class File < StringIO
    def flock(*)
      true
    end
  end
end

RSpec.configure do |config|
  config.include FakeFS::SpecHelpers
  config.include Helpers
end

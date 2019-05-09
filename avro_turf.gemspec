# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'avro_turf/version'

Gem::Specification.new do |spec|
  spec.name          = "avro_turf"
  spec.version       = AvroTurf::VERSION
  spec.authors       = ["Daniel Schierbeck"]
  spec.email         = ["dasch@zendesk.com"]
  spec.summary       = "A library that makes it easier to use the Avro serialization format from Ruby"
  spec.homepage      = "https://github.com/dasch/avro_turf"
  spec.license       = "MIT"

  spec.files         = `git ls-files -z`.split("\x0")
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.test_files    = spec.files.grep(%r{^(test|spec|features)/})
  spec.require_paths = ["lib"]

  spec.add_dependency "avro", ">= 1.7.7", "< 1.9"
  spec.add_dependency "excon", "~> 0.45"

  spec.add_development_dependency "bundler", "~> 1.7"
  spec.add_development_dependency "rake", "~> 10.0"
  spec.add_development_dependency "rspec", "~> 3.2.0"
  spec.add_development_dependency "fakefs", "~> 0.6.7"
  spec.add_development_dependency "webmock"
  spec.add_development_dependency "sinatra"
  spec.add_development_dependency "json_spec"
  spec.add_development_dependency "rack-test"
  spec.add_development_dependency "pry", "~> 0.12.2"

  spec.post_install_message = %{
avro_turf v0.8.0 deprecates the names AvroTurf::SchemaRegistry,
AvroTurf::CachedSchemaRegistry, and FakeSchemaRegistryServer.

Use AvroTurf::ConfluentSchemaRegistry, AvroTurf::CachedConfluentSchemaRegistry,
and FakeConfluentSchemaRegistryServer instead.

See https://github.com/dasch/avro_turf#deprecation-notice
}
end

#!/usr/bin/env ruby
# frozen_string_literal: true

#
# Measures the encoded size of messages of increasing size.

$LOAD_PATH.unshift(File.expand_path("../lib", File.dirname(__FILE__)))

require "benchmark"
require "avro_turf"

sizes = [1, 10, 100, 1_000, 10_000]
avro = AvroTurf.new(schemas_path: File.dirname(__FILE__))

sizes.each do |size|
  data = {
    "name" => "John" * size,
    "address" => {
      "street" => "1st st." * size,
      "city" => "Citytown" * size
    }
  }

  result = avro.encode(data, schema_name: "person")
  encoded_size = result.bytesize
  encode_factor = result.bytesize / size.to_f
  puts "size #{size}: #{encoded_size} bytes (encoding factor #{encode_factor})"
end

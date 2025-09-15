#!/usr/bin/env ruby
# frozen_string_literal: true

#
# Measures the time it takes to encode messages of increasing size.

$LOAD_PATH.unshift(File.expand_path("../lib", File.dirname(__FILE__)))

require 'benchmark'
require 'avro_turf'

# Number of iterations per run.
N = 10_000

Benchmark.bm(15) do |x|
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

    x.report("size #{size}:") do
      N.times { avro.encode(data, schema_name: "person") }
    end
  end
end

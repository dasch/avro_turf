# frozen_string_literal: true

require "avro_turf/cached_confluent_schema_registry"

# AvroTurf::CachedSchemaRegistry is deprecated and will be removed in a future release.
# Use AvroTurf::CachedConfluentSchemaRegistry instead.

AvroTurf::CachedSchemaRegistry = AvroTurf::CachedConfluentSchemaRegistry

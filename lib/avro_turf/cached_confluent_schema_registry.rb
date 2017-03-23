require 'avro_turf/confluent_schema_registry'

# Caches registrations and lookups to the schema registry in memory.
class AvroTurf::CachedConfluentSchemaRegistry
  extend Forwardable
  def_delegators :@upstream,
                 :subjects, :subject_versions, :check, :compatible?,
                 :global_config, :update_global_config, :subject_config,
                 :update_subject_config

  def initialize(upstream)
    @upstream = upstream
    @schemas_by_id = {}
    @schemas_by_subject = {}
    @ids_by_schema = {}
  end

  def fetch(id)
    @schemas_by_id[id] ||= @upstream.fetch(id)
  end

  def subject_version(subject, version = 'latest')
    @schemas_by_subject[subject] ||=
      @upstream.subject_version(subject, version)
  end

  def register(subject, schema)
    @ids_by_schema[subject + schema.to_s] ||= @upstream.register(subject, schema)
  end
end

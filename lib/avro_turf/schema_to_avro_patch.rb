# frozen_string_literal: true

class AvroTurf
  module AvroGemPatch
    module RecordSchema
      module ClassMethods
        def make_field_objects(field_data, names, namespace=nil)
          new_field_data = []
          field_data.each do |field|
            if field.respond_to?(:[]) && !field.key?('default')
              field = field.clone
              field['default'] = :no_default
            end
            new_field_data << field
          end
          super(new_field_data, names, namespace)
        end
      end
      
      def self.prepended(base)
        class << base
          prepend ClassMethods
        end
      end
    end
    
    module Field
      def initialize(type, name, default=:no_default, order=nil, names=nil, namespace=nil)
        super(type, name, default, order, names, namespace)
      end
      
      def to_avro(names=Set.new)
        {'name' => name, 'type' => type.to_avro(names)}.tap do |avro|
          avro['default'] = default unless default == :no_default
          avro['order'] = order if order
        end
      end
    end

    module DatumReader
      def read_default_value(field_schema, default_value)
        if default_value == :no_default
          raise Avro::AvroError, "Missing data for #{field_schema} with no default"
        end

        super
      end
    end
  end
end

Avro::Schema::RecordSchema.send(:prepend, AvroTurf::AvroGemPatch::RecordSchema)
Avro::Schema::Field.send(:prepend, AvroTurf::AvroGemPatch::Field)
Avro::IO::DatumReader.send(:prepend, AvroTurf::AvroGemPatch::DatumReader)

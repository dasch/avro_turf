class AvroTurf
  class Model
    def self.build(avro, schema_name: nil, schema: nil)
      schema ||= avro.find_schema(schema_name)

      Class.new(self) do
        schema.fields.each do |field|
          type = field.type

          attr_accessor field.name

          case type.type_sym
          when :enum
            type.symbols.each do |symbol|
              const_set(symbol.upcase, symbol)
            end
          when :record
            klass = build(avro, schema: type)

            # hello_world -> HelloWorld
            klass_name = type.name.
              split("_").
              map {|word| word[0] = word[0].upcase; word }.
              join

            const_set(klass_name, klass)

            define_method("#{field.name}=") do |value|
              instance_variable_set("@#{field.name}", klass.new(value))
            end
          end
        end
      end
    end

    def initialize(**attributes)
      attributes.each do |attr, value|
        if respond_to?("#{attr}=")
          send("#{attr}=", value)
        else
          raise ArgumentError, "no such attribute `#{attr}`"
        end
      end
    end
  end
end

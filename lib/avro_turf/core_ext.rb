class AvroTurf
  module CoreExt
    refine(String) do
      def as_avro
        self
      end
    end

    refine(Numeric) do
      def as_avro
        self
      end
    end

    refine(Enumerable) do
      def as_avro
        map(&:as_avro)
      end
    end

    refine(Hash) do
      def as_avro
        hsh = Hash.new
        each { |k, v| hsh[k.as_avro] = v.as_avro }
        hsh
      end
    end

    refine(Time) do
      def as_avro
        iso8601
      end
    end

    refine(Date) do
      def as_avro
        iso8601
      end
    end

    refine(Symbol) do
      def as_avro
        to_s
      end
    end

    refine(NilClass) do
      def as_avro
        self
      end
    end

    refine(TrueClass) do
      def as_avro
        self
      end
    end

    refine(FalseClass) do
      def as_avro
        self
      end
    end
  end
end
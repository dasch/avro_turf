module Enumerable
  def as_avro
    map(&:as_avro)
  end
end

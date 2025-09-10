# frozen_string_literal: true

class Hash
  def as_avro
    hsh = Hash.new
    each {|k, v| hsh[k.as_avro] = v.as_avro }
    hsh
  end
end

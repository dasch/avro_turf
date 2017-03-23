require 'logger'

class AvroTurf
  # Allow configuration of AvroTurf
  module Configuration
    attr_writer :logger

    def configure
      yield(self)
    end

    def logger
      @logger ||= Logger.new($stderr)
    end
  end
end

module MoSQL
  module Logging
    def log
      @@logger ||= Log4r::Logger.new("MoSQL")
    end
  end
end

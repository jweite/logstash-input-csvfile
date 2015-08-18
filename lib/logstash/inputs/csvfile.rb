# encoding: utf-8
require "logstash/inputs/file"
require "logstash/namespace"

class LogStash::Inputs::CSVFile < LogStash::Inputs::File
	config_name "csvfile"

	def decorate(event)
		super(event)
		event["_csv"] = true
	end

end # class LogStash::Inputs::CSVFile
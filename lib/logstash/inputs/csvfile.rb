# encoding: utf-8
require "logstash/inputs/file"
require "logstash/namespace"
require "csv"

class LogStash::Inputs::CSVFile < LogStash::Inputs::File
	config_name "csvfile"

  # Define a list of column names (in the order they appear in the CSV,
  # as if it were a header line). If `columns` is not configured, or there
  # are not enough columns specified, the default column names are
  # "column1", "column2", etc. In the case that there are more columns
  # in the data than specified in this column list, extra columns will be auto-numbered:
  # (e.g. "user_defined_1", "user_defined_2", "column3", "column4", etc.)
  config :columns, :validate => :array, :default => []

  # Bool flag enables sourcing of column names from first event (line) of each file.
  #  A dynamic alternative to explicitly defining columns in the column attribute
  config :first_line_defines_columns, :validate => :boolean, :default => false

  # Define the column separator value. If this is not specified, the default
  # is a comma `,`.
  # Optional.
  config :separator, :validate => :string, :default => ","

  # Define the character used to quote CSV fields. If this is not specified
  # the default is a double quote `"`.
  # Optional.
  config :quote_char, :validate => :string, :default => '"'

  # Define target field for placing the data.
  # Defaults to writing to the root of the event.
  config :target, :validate => :string

  public
  def register
    @fileColumns = Hash.new			# Hash to hold the col schema of each known file
    super()
  end
  
  def decorate(event)
    super(event)
	
	messageTag = "message"
	pathTag = "path"

    if event[messageTag]	
    
      begin
        values = CSV.parse_line(event[messageTag], :col_sep => @separator, :quote_char => @quote_char)

        if @target.nil?
          # Default is to write to the root of the event.
          dest = event
        else
          dest = event[@target] ||= {}
        end

		# Get attribute names for the columns.
		cols = []
		if @first_line_defines_columns
			# See whether we've seen event from this file before
			if !@fileColumns.has_key?(event[pathTag])
				#Event's path is unknown in schema hash.  This must be first event for this file, defining its columns.  Remember them.
				@fileColumns[event[pathTag]] = values
				event["_csvmetadata"] = true 				# Flag enables subsequent filtering of csv metadata line
			else				
				#We know this file's columns.  Use them.
				cols = @fileColumns[event[pathTag]]
			end
		else
			# Use explicitly defined columns
			cols = @columns 
		end

		# Don't add column attributes if this is the metadata event.
        if !event["_csvmetadata"]
			values.each_index do |i|
				field_name = cols[i] || "column#{i+1}"
				dest[field_name] = values[i]
			end
		end
		
      rescue => e
        event.tag "_csvparsefailure"
        @logger.warn("Trouble parsing csv", messageTag => event[messageTag], 
                      :exception => e)
        return
      end # begin
    end # if event[messageTag]
  end # decorate()
end # class LogStash::Inputs::CSVFile

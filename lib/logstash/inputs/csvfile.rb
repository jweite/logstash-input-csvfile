# encoding: utf-8
require "logstash/inputs/file"
require "logstash/namespace"
require "csv"

# Subclass of logstash-input-file that parses CSV lines.
#
# Unlike logstash-filter-csv, this version can respect a first-line schema in the csv,
# in which the first line of the CSV file defines the column names for the remainder.
# Set plugin config first_line_defines_columns to true to enable this behavior.
#
# (Alternatively one can statically define a list of columns in the columns config
# as logstash-filter-csv does).  
#
# Since multiple files may be being read by the same plugin instance, and each can have
# a distinct schema, this plugin records the schema for each file (as defined by the
# event's path attribute) in a hash.  When it receives an event for a file it doesn't
# know it reads/parses that file's first line to obtain the schema.

# I considered extending logstash-filter-csv for this, but 

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
    @fileColumns = Hash.new     # Hash to hold the col schema of each known file
    super()
  end
  
  def decorate(event)
    super(event)
  
    message = event["message"]
    path = event["path"]
    
    @logger.debug? && @logger.debug("handling csv in first_line_defines_schema mode", :path => path, :message => message)
    
    if message
      begin
        values = CSV.parse_line(message, :col_sep => @separator, :quote_char => @quote_char)

        if @target.nil?
          # Default is to write to the root of the event.
          dest = event
        else
          dest = event[@target] ||= {}
        end

        # Get attribute names for the columns.
        cols = []
        if @first_line_defines_columns
          if !path
            @logger.warn("No path in event.  Cannot use first_line_defines_columns mode on this event.")
            cols = []   #Parse with default col names
          else
            @logger.debug? && @logger.debug("handling csv in first_line_defines_schema mode", :path => path, :message => message)
            if !@fileColumns.has_key?(path)   
              @logger.debug? && @logger.debug("Unknown file/schema.  Reading schema from file.", :path => path)
              firstLine = ""
              File.open(path, "r") do |f|
                firstLine = f.gets
              end
              @fileColumns[path] = CSV.parse_line(firstLine, :col_sep => @separator, :quote_char => @quote_char)
              @logger.debug? && @logger.debug("Schema read from file:", :path => path, :cols => @fileColumns[path])
              
              if @fileColumns[path].join == values.join
                @logger.debug? && @logger.debug("Received the schema row event.  Tagging w/ _csvmetadata", :message => message)
                cols = []
                event["_csvmetadata"] = true        # Flag enables subsequent filtering of csv metadata line
              else
                cols = @fileColumns[path]
              end
            else        
              #We know this file's columns.  Use them.
              @logger.debug? && @logger.debug("Known file. Using cached schema", :cols => @fileColumns[path])
              cols = @fileColumns[path]
            end
          end
        else
          @logger.debug? && @logger.debug("handling csv in explicitly defined columns mode", :message => message, :columns => @columns)
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
        @logger.warn("Trouble parsing csv", :message => message, :exception => e)
        return
      end # begin
    end # if message
  end # decorate()
end # class LogStash::Inputs::CSVFile

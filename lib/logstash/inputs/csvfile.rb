# encoding: utf-8
require "logstash/inputs/file"
require "logstash/namespace"
require "csv"

# Subclass of logstash-input-file that parses CSV lines, with support for first-line schemas.
# Set first_line_defines_columns => true to enable this behavior.
# Statically defined columns are also supported, a la logstash-filter-csv, via the columns param.
# first_line_defines_columns => true takes precedence, though.
#
# Since multiple files may be being read by the same plugin instance, and each can have
# a distinct schema, this plugin records a schema per source file (as defined by the
# event's path attribute) in a hash.  When it receives an event for a file it doesn't
# know it reads/parses that file's first line to obtain the schema.  This method supports 
# resuming processing after logstash restarts in mid-file.
#
# I considered extending logstash-filter-csv for to do this, but felt that the only reliable
# way to support streaming csv read was to explicitly read it from the file's schema row 
# (and cache it so subsequent row performance for that file is good.)  Since we cannot count 
# on a logstash filter having read-access to the file, or even processing events that originate
# from files I rejected this approach.  By definition, a file input plugin must have read-access
# to the file it's sourcing data from.
#
# This plugin borrows most of its csv parsing logic from logstash-filter-csv.  
#
# This plugin extends logstash-input-file by overriding its decorate method.  Note that
# logstash-input-plugin 0.0.10, released with Logstash 1.5, doesn't set the event's 
# path element before calling decorate (which this plugin requires), so gemspec insists
# on logstash-input-file 1.1.0  
#

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

  # The maximum time a csv file's schema can be unused (in hours) before 
  # it is automatically scrubbed to avoid memory leakage.  
  # If an event for that file arrives subsequently the schema will be 
  # reconstituted (albeit with the penalty of schema row re-read from file).
  #
  # Cache scrubbing occurs inline only when new new files are detected to minimize
  # perf impact on most CSV events.  Since new file detection time is the only time 
  # the cache actually grows, and we're expecting to pay the schema-read penalty then 
  # anyway, it's an optimal time to scrub.
  #
  # 0 disables, but memory will grow.  OK if you're routinely restarting logstash.
  config :max_cached_schema_age_hours, :validate => :number, :default => 24
  
  # To handle cases where there's other content in the file before the schema row
  # that you'll want to ignore. For instance, you can skip leading blank lines 
  # before the schema by matching to non-blank lines using "^.+$"
  # Note that the plugin will still emit events for pre-schema rows, albeit with
  # no attributes (for blank lines) or default-named attributes (if the pre-schema
  # lines do parse as valid CSV).
  config :schema_pattern_to_match, :validate => :string

  # To support testing.  Adds attributes to events regarding schema cache behavior.
  config :add_schema_cache_telemetry_to_event, :validate => :boolean, :default => false
  
  public
  def register
    @fileColumns = Hash.new
    @schemaTouchedTimes = Hash.new
    super()
    
    @logger.warn("schema cache scrubbing disabled.  Memory use will grow over time.") if @max_cached_schema_age_hours <= 0
  end
  
  def decorate(event)
    super(event)
  
    message = event.get("message")
    return if !message
    
    begin
      values = CSV.parse_line(message, :col_sep => @separator, :quote_char => @quote_char)
      return if values.length == 0 

      # Get names for the columns.
      if @first_line_defines_columns
        @logger.debug? && @logger.debug("handling csv in first_line_defines_columns mode", :message => message, :columns => @columns)
        cols = getSchemaForFile(event, values)
      else
        @logger.debug? && @logger.debug("handling csv in explicitly defined columns mode", :message => message, :columns => @columns)
        cols = @columns 
      end

      # Determine where to write the new attributes
      if @target.nil?
        # Default is to write to the root of the event.
        dest = event
      else
        dest = event.get(@target)
		dest = {} if !dest
      end

      # Add the per-column attributes (as long as this isn't the event from the schema defining row)
      if !event.get("_csvmetadata")
        values.each_index do |i|
        field_name = cols[i] || "column#{i+1}"
        dest.set(field_name, values[i])
      end
    end

    rescue => e
      event.tag "_csvparsefailure"
      @logger.warn("Trouble parsing csv", :message => message, :exception => e)
      return
    end # begin
  end # decorate()

  def getSchemaForFile(event, parsedValues)
    path = event.get("path")
    if !path
      @logger.warn("No path in event.  Cannot retrieve a schema for this event.")
      return []
    end

    @logger.debug? && @logger.debug("Getting schema for file", :path => path)

    schema = getCachedSchemaForFile(path)
    if schema   
      @logger.debug? && @logger.debug("Using cached schema", :cols => schema)
      event.set("_schemacachetelemetry", "cachedEntryUsed") if @add_schema_cache_telemetry_to_event  
      touchSchema(path)
      return schema
    end

    @logger.debug? && @logger.debug("Event from unknown file/schema.  Reading schema from that file.", :path => path)

    scrubSchemaCache(event) if @max_cached_schema_age_hours > 0

    csvFileLine = readSchemaLineFromFile(path)
    if !csvFileLine || csvFileLine.length == 0 
      @logger.warn("No suitable schema row found in file.", :path => path)
      return []
    end

    schema = CSV.parse_line(csvFileLine, :col_sep => @separator, :quote_char => @quote_char)
    addSchemaToCache(path, schema)
    @logger.debug? && @logger.debug("Schema read from file:", :path => path, :cols => schema)
    
    if @add_schema_cache_telemetry_to_event 
      event.set("_schemacachetelemetry", "newEntryCreated")
      event.set("_cache_touch_time", Time.now)
    end
     
    # Special handling for the schema row event: tag _csvmetadata and don't return individual column attributes
    if @fileColumns[path].join == parsedValues.join
      @logger.debug? && @logger.debug("Received the schema row event.  Tagging w/ _csvmetadata", :message => message)
      event.set("_csvmetadata", true)
      return []
    else 
      return schema
    end
    
  end
  
  def getCachedSchemaForFile(path)
    @fileColumns[path]
  end
  
  def addSchemaToCache(path, schema)
    @fileColumns[path] = schema
    touchSchema(path)
  end

  def touchSchema(path)
    @schemaTouchedTimes[path] = Time.now
  end
  
  def readSchemaLineFromFile(path)
    csvFileLine = ""
    File.open(path, "r") do |f| 
      while csvFileLine.length == 0 and csvFileLine = f.gets 
        if @schema_pattern_to_match
          if !csvFileLine.end_with?("\n") or !csvFileLine.match(@schema_pattern_to_match) 
            csvFileLine = ""
          end
        end
      end
    end
    csvFileLine
  end
  
  def scrubSchemaCache(event)
    @logger.debug? && @logger.debug("Scrubbing schema cache", :size => @fileColumns.length)
    event.set("_schemacachetelemetryscrubbedbeforecount", @fileColumns.length) if @add_schema_cache_telemetry_to_event 
    
    expiringFiles = []
    now = Time.now
    @schemaTouchedTimes.each do |filename, lastReadTime|
      if (lastReadTime + (@max_cached_schema_age_hours * 60 * 60)) < now
        expiringFiles << filename 
        @logger.debug? && @logger.debug("Expiring schema for: ", :file => filename, :lastRead => lastReadTime)
      end
    end
    
    expiringFiles.each do |filename|
      @fileColumns.delete(filename)
      @schemaTouchedTimes.delete(filename)
      @logger.debug? && @logger.debug("Deleted schema for: ", :file => filename)
    end

    event.set("_schemacachetelemetryscrubbedaftercount", @fileColumns.length) if @add_schema_cache_telemetry_to_event 
    @logger.debug? && @logger.debug("Done scrubbing schema cache", :size => @fileColumns.length)
    
  end
  
end # class LogStash::Inputs::CSVFile

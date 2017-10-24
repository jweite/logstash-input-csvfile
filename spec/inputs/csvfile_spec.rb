# encoding: utf-8

require "logstash/devutils/rspec/spec_helper"
require "tempfile"
require "stud/temporary"
require "logstash/inputs/csvfile"

describe "inputs/csvfile" do

  delimiter = (LogStash::Environment.windows? ? "\r\n" : "\n")

  #Borrowed this first check from file_spec.rb verbatim to get the pipeline running...
  it "should starts at the end of an existing file" do
    tmpfile_path = Stud::Temporary.pathname
    sincedb_path = Stud::Temporary.pathname

    conf = <<-CONFIG
      input {
        file {
          type => "blah"
          path => "#{tmpfile_path}"
          sincedb_path => "#{sincedb_path}"
          delimiter => "#{delimiter}"
        }
      }
    CONFIG

    File.open(tmpfile_path, "w") do |fd|
      fd.puts("ignore me 1")
      fd.puts("ignore me 2")
    end

    events = input(conf) do |pipeline, queue|

      # at this point the plugins
      # threads might still be initializing so we cannot know when the
      # file plugin will have seen the original file, it could see it
      # after the first(s) hello world appends below, hence the
      # retry logic.

      events = []

      retries = 0
      while retries < 20
        File.open(tmpfile_path, "a") do |fd|
          fd.puts("hello")
          fd.puts("world")
        end

        if queue.size >= 2
          events = 2.times.collect { queue.pop }
          break
        end

        sleep(0.1)
        retries += 1
      end

      events
    end #input block

    insist { events[0].get("message") } == "hello"
    insist { events[1].get("message") } == "world"
  end #it
  
  it "should parse csv columns into event attributes using default column names" do
    tmpfile_path = Stud::Temporary.pathname
    sincedb_path = Stud::Temporary.pathname

    conf = <<-CONFIG
      input {
        csvfile {
          path => "#{tmpfile_path}"
          start_position => "beginning"
          sincedb_path => "#{sincedb_path}"
          delimiter => "#{delimiter}"
        }
      }
    CONFIG

    File.open(tmpfile_path, "a") do |fd|
      fd.puts("first,second,third")
      fd.puts('"fou,rth","fifth"')              #Quoting check
      fd.puts("sixth,seventh,eighth,ninth")
    end

    events = input(conf) do |pipeline, queue|
      3.times.collect { queue.pop }
    end

    insist { events[0].get("column1") } == "first"
    insist { events[0].get("column2") } == "second"
    insist { events[0].get("column3") } == "third"
    insist { events[1].get("column1") } == "fou,rth"  #Not a typo: quoting check
    insist { events[1].get("column2") } == "fifth"
    insist { events[2].get("column1") } == "sixth"
    insist { events[2].get("column2") } == "seventh"
    insist { events[2].get("column3") } == "eighth"
    insist { events[2].get("column4") } == "ninth"

  end #it

  it "should parse csv columns into attributes using explicitly defined column names, default-naming any excess columns; non-default csv separator" do
    tmpfile_path = Stud::Temporary.pathname
    sincedb_path = Stud::Temporary.pathname

    conf = <<-CONFIG
      input {
        csvfile {
          path => "#{tmpfile_path}"
          start_position => "beginning"
          sincedb_path => "#{sincedb_path}"
          delimiter => "#{delimiter}"
          separator => ";"
          columns => ["FIRST_COL","SECOND_COL","THIRD_COL"]
        }
      }
    CONFIG

    File.open(tmpfile_path, "a") do |fd|
      fd.puts("first;second;third")
      fd.puts("fourth;fifth")
      fd.puts("sixth;sev,enth;eighth;ninth")
    end

    events = input(conf) do |pipeline, queue|
      3.times.collect { queue.pop }
    end

    insist { events[0].get("FIRST_COL") } == "first"
    insist { events[0].get("SECOND_COL") } == "second"
    insist { events[0].get("THIRD_COL") } == "third"
    insist { events[1].get("FIRST_COL") } == "fourth"
    insist { events[1].get("SECOND_COL") } == "fifth"
    insist { events[2].get("FIRST_COL") } == "sixth"
    insist { events[2].get("SECOND_COL") } == "sev,enth"
    insist { events[2].get("THIRD_COL") } == "eighth"
    insist { events[2].get("column4") } == "ninth"

  end #it

  it "should parse csv columns into attributes using column names defined on the csv files 0th row with each csv file defining its own independent schema; it should tag schema row events as _csvmetadata" do
    tmpfile_path = Stud::Temporary.pathname
    tmpfile2_path = Stud::Temporary.pathname
    sincedb_path = Stud::Temporary.pathname

    conf = <<-CONFIG
      input {
        csvfile {
          path => "#{tmpfile_path}"
          path => "#{tmpfile2_path}"
          start_position => "beginning"
          sincedb_path => "#{sincedb_path}"
          delimiter => "#{delimiter}"
       	  first_line_defines_columns => true
        }
      }
    CONFIG

    File.open(tmpfile_path, "a") do |fd|
      fd.puts("A_COLUMN,B_COLUMN,C_COLUMN")
      fd.puts("first,second,third")
      fd.puts("fourth,fifth")
      fd.puts("sixth,seventh,eighth,ninth")
    end

    events = input(conf) do |pipeline, queue|
      4.times.collect { queue.pop }
    end

    insist { events[0].get("_csvmetadata") } == true
    insist { events[1].get("A_COLUMN") } == "first"
    insist { events[1].get("B_COLUMN") } == "second"
    insist { events[1].get("C_COLUMN") } == "third"
    insist { events[2].get("A_COLUMN") } == "fourth"
    insist { events[2].get("B_COLUMN") } == "fifth"
    insist { events[3].get("A_COLUMN") } == "sixth"
    insist { events[3].get("B_COLUMN") } == "seventh"
    insist { events[3].get("C_COLUMN") } == "eighth"
    insist { events[3].get("column4") } == "ninth"

    File.open(tmpfile2_path, "a") do |fd|
      fd.puts("D_COLUMN,E_COLUMN,F_COLUMN")
      fd.puts("first,second,third")
      fd.puts("fourth,fifth")
      fd.puts("sixth,seventh,eighth,ninth")
    end

    events = input(conf) do |pipeline, queue|
      4.times.collect { queue.pop }
    end

    insist { events[0].get("_csvmetadata") } == true
    insist { events[1].get("D_COLUMN") } == "first"
    insist { events[1].get("E_COLUMN") } == "second"
    insist { events[1].get("F_COLUMN") } == "third"
    insist { events[2].get("D_COLUMN") } == "fourth"
    insist { events[2].get("E_COLUMN") } == "fifth"
    insist { events[3].get("D_COLUMN") } == "sixth"
    insist { events[3].get("E_COLUMN") } == "seventh"
    insist { events[3].get("F_COLUMN") } == "eighth"
    insist { events[3].get("column4") } == "ninth"

  end #it

  it "should parse csv columns into attributes using column names defined on the first file row that matches the schema_pattern_to_match with each csv file defining its own independent schema; it should tag schema row events as _csvmetadata" do
    tmpfile_path = Stud::Temporary.pathname
    sincedb_path = Stud::Temporary.pathname

    conf = <<-CONFIG
      input {
        csvfile {
          path => "#{tmpfile_path}"
          start_position => "beginning"
          sincedb_path => "#{sincedb_path}"
          delimiter => "#{delimiter}"
       	  first_line_defines_columns => true
          schema_pattern_to_match => "^.+$"
        }
      }
    CONFIG

    File.open(tmpfile_path, "a") do |fd|
      fd.puts("")
      fd.puts("A_COLUMN,B_COLUMN,C_COLUMN")
      fd.puts("first,second,third")
      fd.puts("fourth,fifth")
      fd.puts("sixth,seventh,eighth,ninth")
    end

    events = input(conf) do |pipeline, queue|
      5.times.collect { queue.pop }
    end

    insist { events[1].get("_csvmetadata") } == true
    insist { events[2].get("A_COLUMN") } == "first"
    insist { events[2].get("B_COLUMN") } == "second"
    insist { events[2].get("C_COLUMN") } == "third"
    insist { events[3].get("A_COLUMN") } == "fourth"
    insist { events[3].get("B_COLUMN") } == "fifth"
    insist { events[4].get("A_COLUMN") } == "sixth"
    insist { events[4].get("B_COLUMN") } == "seventh"
    insist { events[4].get("C_COLUMN") } == "eighth"
    insist { events[4].get("column4") } == "ninth"

  end #it

  it "should parse csv columns into attributes using explicitly defined column names, default-naming any excess columns; non-default csv separator" do
    tmpfile_path = Stud::Temporary.pathname
    sincedb_path = Stud::Temporary.pathname

    conf = <<-CONFIG
      input {
        csvfile {
          path => "#{tmpfile_path}"
          start_position => "beginning"
          sincedb_path => "#{sincedb_path}"
          delimiter => "#{delimiter}"
          separator => ";"
          columns => ["FIRST_COL","SECOND_COL","THIRD_COL"]
        }
      }
    CONFIG

    File.open(tmpfile_path, "a") do |fd|
      fd.puts("first;second;third")
      fd.puts("fourth;fifth")
      fd.puts("sixth;sev,enth;eighth;ninth")
    end

    events = input(conf) do |pipeline, queue|
      3.times.collect { queue.pop }
    end

    insist { events[0].get("FIRST_COL") } == "first"
    insist { events[0].get("SECOND_COL") } == "second"
    insist { events[0].get("THIRD_COL") } == "third"
    insist { events[1].get("FIRST_COL") } == "fourth"
    insist { events[1].get("SECOND_COL") } == "fifth"
    insist { events[2].get("FIRST_COL") } == "sixth"
    insist { events[2].get("SECOND_COL") } == "sev,enth"
    insist { events[2].get("THIRD_COL") } == "eighth"
    insist { events[2].get("column4") } == "ninth"

  end #it

  it "should cache schemas per file" do
    tmpfile_path = Stud::Temporary.pathname
    tmpfile2_path = Stud::Temporary.pathname
    sincedb_path = Stud::Temporary.pathname

    conf = <<-CONFIG
      input {
        csvfile {
          path => "#{tmpfile_path}"
          path => "#{tmpfile2_path}"
          start_position => "beginning"
          sincedb_path => "#{sincedb_path}"
          delimiter => "#{delimiter}"
       	  first_line_defines_columns => true
          add_schema_cache_telemetry_to_event => true
        }
      }
    CONFIG

   
    events = input(conf) do |pipeline, queue|
      File.open(tmpfile_path, "a") do |fd|
        fd.puts("A_COLUMN,B_COLUMN,C_COLUMN")
        fd.puts("first,second,third")
      end

      sleep 1
      
      File.open(tmpfile2_path, "a") do |fd|
        fd.puts("D_COLUMN,E_COLUMN,F_COLUMN")
        fd.puts("1st,2nd,3rd")
      end

      4.times.collect { queue.pop }
    end

    insist { events[0].get("_csvmetadata") } == true
    insist { events[0].get("_schemacachetelemetry") } == "newEntryCreated"
    
    insist { events[1].get("A_COLUMN") } == "first"
    insist { events[1].get("B_COLUMN") } == "second"
    insist { events[1].get("C_COLUMN") } == "third"
    insist { events[1].get("_schemacachetelemetry") } == "cachedEntryUsed"
    
    insist { events[2].get("_csvmetadata") } == true
    insist { events[2].get("_schemacachetelemetry") } == "newEntryCreated"
    
    insist { events[3].get("D_COLUMN") } == "1st"
    insist { events[3].get("E_COLUMN") } == "2nd"
    insist { events[3].get("F_COLUMN") } == "3rd"
    insist { events[3].get("_schemacachetelemetry") } == "cachedEntryUsed"

  end #it

  it "should resume processing of a csv file after logstash restarts" do
    tmpfile_path = Stud::Temporary.pathname
    sincedb_path = Stud::Temporary.pathname

    # Set up to expire cache entries after 10s of being untouched.  Request that telemetry be added to the event to make cache usage visible.
    conf = <<-CONFIG
      input {
        csvfile {
          path => "#{tmpfile_path}"
          start_position => "beginning"
          sincedb_path => "#{sincedb_path}"
          delimiter => "#{delimiter}"
       	  first_line_defines_columns => true
          add_schema_cache_telemetry_to_event => true
        }
      }
    CONFIG

   
    File.open(tmpfile_path, "a") do |fd|
      fd.puts("A_COLUMN,B_COLUMN,C_COLUMN")
      fd.puts("first,second,third")
    end

    events = input(conf) do |pipeline, queue|
      2.times.collect { queue.pop }
    end

    insist { events[0].get("_csvmetadata") } == true
    insist { events[0].get("_schemacachetelemetry") } == "newEntryCreated"
    
    insist { events[1].get("A_COLUMN") } == "first"
    insist { events[1].get("B_COLUMN") } == "second"
    insist { events[1].get("C_COLUMN") } == "third"
    insist { events[1].get("_schemacachetelemetry") } == "cachedEntryUsed"
    
    File.open(tmpfile_path, "a") do |fd|
      fd.puts("fourth,fifth,sixth")
    end

    events = input(conf) do |pipeline, queue|
      1.times.collect { queue.pop }
    end

    insist { events[0].get("A_COLUMN") } == "fourth"
    insist { events[0].get("B_COLUMN") } == "fifth"
    insist { events[0].get("C_COLUMN") } == "sixth"
    insist { events[0].get("_schemacachetelemetry") } == "newEntryCreated"

  end #it

  it "should expire schema cache entries if untouched for more than their configured lifetime (10s in this case)" do
    
    # This was tricky to write.  Key points:
    # - Utilizes a special white-box mode of the plugin that exposes what its doing with its schema cache in telemetry attributes.
    # - While cache durations are typically in multiple hours, for testing we dial it back to 10s via a small fractional number.
    # - All the various file IO has to go into the input block
    # - The queue reads are sprinkled throughout to synchronize the test proc with logstash's file processing.
    # - Put the insists right after the queue reads to better tie the inputs with the expected outputs.
  
    puts "\nThe caching test now running will take a while... (~30s)"
    
    tmpfile_path = Stud::Temporary.pathname
    tmpfile2_path = Stud::Temporary.pathname
    tmpfile3_path = Stud::Temporary.pathname
    sincedb_path = Stud::Temporary.pathname

    conf = <<-CONFIG
      input {
        csvfile {
          path => "#{tmpfile_path}"
          path => "#{tmpfile2_path}"
          path => "#{tmpfile3_path}"
          start_position => "beginning"
          sincedb_path => "#{sincedb_path}"
          delimiter => "#{delimiter}"
       	  first_line_defines_columns => true
          max_cached_schema_age_hours => 0.0027777777777778
          add_schema_cache_telemetry_to_event => true
          discover_interval => 1
        }
      }
    CONFIG
   
    events = input(conf) do |pipeline, queue|
    
      # File1 Initial Entries.  File 1's schema will be cached.
      File.open(tmpfile_path, "a") do |fd|
        fd.puts("A_COLUMN,B_COLUMN,C_COLUMN")
        fd.puts("first,second,third")
      end
      # Verify File1 schema was cached and schema row was tagged as csvmetadata
      event = queue.pop
	  
      insist { event.get("_schemacachetelemetry") } == "newEntryCreated"
      insist { event.get("_csvmetadata") } == true

      # Verify that cached File1 schema was used to decode row2 of File1
      event = queue.pop
      insist { event.get("_schemacachetelemetry") } == "cachedEntryUsed"
      insist { event.get("A_COLUMN") } == "first"
      insist { event.get("B_COLUMN") } == "second"
      insist { event.get("C_COLUMN") } == "third"

      # File2 Initial Entries
      File.open(tmpfile2_path, "a") do |fd|
        fd.puts("D_COLUMN,E_COLUMN,F_COLUMN")
        fd.puts("1st,2nd,3rd")
      end
      # Verify File2 schema was cached and schema row was tagged as csvmetadata
      event = queue.pop
      insist { event.get("_schemacachetelemetry") } == "newEntryCreated"
      insist { event.get("_csvmetadata") } == true
      
      # Verify that cached File2 schema was used to decode row2 of File2
      event = queue.pop
      insist { event.get("_schemacachetelemetry") } == "cachedEntryUsed"
      insist { event.get("D_COLUMN") } == "1st"
      insist { event.get("E_COLUMN") } == "2nd"
      insist { event.get("F_COLUMN") } == "3rd"

      # Touch File1 before its cached schema entries expires (<10s), refreshing the entry.
      sleep 5
      File.open(tmpfile_path, "a") do |fd|
        fd.puts("fourth,fifth,sixth")
      end
      # Verify that still-cached File1 schema was used to decode newly added row of File1
      event = queue.pop
      insist { event.get("_schemacachetelemetry") } == "cachedEntryUsed"
      insist { event.get("A_COLUMN") } == "fourth"
      insist { event.get("B_COLUMN") } == "fifth"
      insist { event.get("C_COLUMN") } == "sixth"

      # Touch File1 again after File2's cache entry expires.  
      sleep 10
      File.open(tmpfile_path, "a") do |fd|
        fd.puts("seventh,eighth,ninth")
      end
      # Verify that File1's entry hasn't expired, by virtue of the previous touch refreshing it.
      event = queue.pop
      insist { event.get("_schemacachetelemetry") } == "cachedEntryUsed"
      insist { event.get("A_COLUMN") } == "seventh"
      insist { event.get("B_COLUMN") } == "eighth"
      insist { event.get("C_COLUMN") } == "ninth"

      # Touch File3. Creation of its cache entry forces purge of File2's expired entry, which is made visible via telemetry.
      sleep 1
      File.open(tmpfile3_path, "a") do |fd|
        fd.puts("X_COLUMN,Y_COLUMN,Z_COLUMN")
        fd.puts("erste,zweite,dritte")
      end
      # Verify that scrubbing of expired cache entries takes place, reducing cached count from 2 (File1 & File2) to 1 (Just File1).
      #  (Scrubbing takes place before creation of File3's schema entry in the cache.)
      event = queue.pop
      insist { event.get("_csvmetadata") } == true
      insist { event.get("_schemacachetelemetry") } == "newEntryCreated"
      insist { event.get("_schemacachetelemetryscrubbedbeforecount") } == 2
      insist { event.get("_schemacachetelemetryscrubbedaftercount") } == 1

      # Verify that File3's schema did in fact get cached.
      event = queue.pop
      insist { event.get("_schemacachetelemetry") } == "cachedEntryUsed"
      insist { event.get("X_COLUMN") } == "erste"
      insist { event.get("Y_COLUMN") } == "zweite"
      insist { event.get("Z_COLUMN") } == "dritte"
      
      # File2 post-expiration entry.  Should re-create the File2 cache entry.
      sleep 1
      File.open(tmpfile2_path, "a") do |fd|
        fd.puts("4th,5th,6th")
      end
      # Verify that File2's schema gets recreated (but not transmitted as an event since this isn't the natural row0 read).
      event = queue.pop
      insist { event.get("_schemacachetelemetry") } == "newEntryCreated"
      insist { event.get("D_COLUMN") } == "4th"
      insist { event.get("E_COLUMN") } == "5th"
      insist { event.get("F_COLUMN") } == "6th"

    end #input block
  end #it
  
end

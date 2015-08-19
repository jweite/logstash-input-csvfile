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
    end

    insist { events[0]["message"] } == "hello"
    insist { events[1]["message"] } == "world"
  end
  
  it "should parse csv columns into attributes using default column names" do
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
      fd.puts('"fou,rth","fifth"')              #Implicit quoting check
      fd.puts("sixth,seventh,eighth,ninth")
    end

    events = input(conf) do |pipeline, queue|
      3.times.collect { queue.pop }
    end

    insist { events[0]["column1"] } == "first"
    insist { events[0]["column2"] } == "second"
    insist { events[0]["column3"] } == "third"
    insist { events[1]["column1"] } == "fou,rth"  #Not a typo.
    insist { events[1]["column2"] } == "fifth"
    insist { events[2]["column1"] } == "sixth"
    insist { events[2]["column2"] } == "seventh"
    insist { events[2]["column3"] } == "eighth"
    insist { events[2]["column4"] } == "ninth"

  end

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

    insist { events[0]["FIRST_COL"] } == "first"
    insist { events[0]["SECOND_COL"] } == "second"
    insist { events[0]["THIRD_COL"] } == "third"
    insist { events[1]["FIRST_COL"] } == "fourth"
    insist { events[1]["SECOND_COL"] } == "fifth"
    insist { events[2]["FIRST_COL"] } == "sixth"
    insist { events[2]["SECOND_COL"] } == "sev,enth"
    insist { events[2]["THIRD_COL"] } == "eighth"
    insist { events[2]["column4"] } == "ninth"

  end

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

    insist { events[0]["_csvmetadata"] } == true
    insist { events[1]["A_COLUMN"] } == "first"
    insist { events[1]["B_COLUMN"] } == "second"
    insist { events[1]["C_COLUMN"] } == "third"
    insist { events[2]["A_COLUMN"] } == "fourth"
    insist { events[2]["B_COLUMN"] } == "fifth"
    insist { events[3]["A_COLUMN"] } == "sixth"
    insist { events[3]["B_COLUMN"] } == "seventh"
    insist { events[3]["C_COLUMN"] } == "eighth"
    insist { events[3]["column4"] } == "ninth"

    File.open(tmpfile2_path, "a") do |fd|
      fd.puts("D_COLUMN,E_COLUMN,F_COLUMN")
      fd.puts("first,second,third")
      fd.puts("fourth,fifth")
      fd.puts("sixth,seventh,eighth,ninth")
    end

    events = input(conf) do |pipeline, queue|
      4.times.collect { queue.pop }
    end

    insist { events[0]["_csvmetadata"] } == true
    insist { events[1]["D_COLUMN"] } == "first"
    insist { events[1]["E_COLUMN"] } == "second"
    insist { events[1]["F_COLUMN"] } == "third"
    insist { events[2]["D_COLUMN"] } == "fourth"
    insist { events[2]["E_COLUMN"] } == "fifth"
    insist { events[3]["D_COLUMN"] } == "sixth"
    insist { events[3]["E_COLUMN"] } == "seventh"
    insist { events[3]["F_COLUMN"] } == "eighth"
    insist { events[3]["column4"] } == "ninth"

  end

end

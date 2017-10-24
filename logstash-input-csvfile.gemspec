Gem::Specification.new do |s|
  s.name = 'logstash-input-csvfile'
  s.version = '0.0.7'
  s.licenses = ['Apache License (2.0)']
  s.summary = "Extends logstash-input-file to parse csv files, optionally respecting 'first-line schemas'"
  s.description = "This gem is a logstash plugin required to be installed on top of the Logstash core pipeline using $LS_HOME/bin/plugin install gemname. This gem is not a stand-alone program"
  s.authors = ["jweite"]
  s.email = 'jweite@yahoo.com'
  s.homepage = ""
  s.require_paths = ["lib"]

  # Files
  s.files = Dir['lib/**/*','spec/**/*']
   # Tests
  s.test_files = s.files.grep(%r{^(test|spec|features)/})

  # Special flag to let us know this is actually a logstash plugin
  s.metadata = { "logstash_plugin" => "true", "logstash_group" => "input" }

  # Gem dependencies
  s.add_runtime_dependency "logstash-core-plugin-api", '>= 1.60', '<= 2.99'
  s.add_runtime_dependency 'logstash-codec-plain'
  s.add_runtime_dependency 'logstash-input-file', '>= 4.0.3'
  s.add_runtime_dependency 'stud'
  s.add_development_dependency 'logstash-devutils', '= 1.3.4'
end

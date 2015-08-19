# logstash-input-csvfile
CSV File Input for Logstash

This plugin extends logstash-input-file, adding CSV parsing logic that can be "first-row-schema" aware.
Enable first-row schema support by setting *first_line_defines_columns* => true.
Statically defined column support(a la logstash-filter-csv) is also supported via the *columns* parameter.  If both are defined first-row schema support takes precedence.


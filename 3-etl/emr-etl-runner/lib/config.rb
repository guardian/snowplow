# Copyright (c) 2012 SnowPlow Analytics Ltd. All rights reserved.
#
# This program is licensed to you under the Apache License Version 2.0,
# and you may not use this file except in compliance with the Apache License Version 2.0.
# You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the Apache License Version 2.0 is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.

# Author::    Alex Dean (mailto:support@snowplowanalytics.com)
# Copyright:: Copyright (c) 2012 SnowPlow Analytics Ltd
# License::   Apache License Version 2.0

require 'optparse'
require 'date'
require 'yaml'

# Config module to hold functions related to CLI argument parsing
# and config file reading to support the daily ETL job.
module Config

  # What are we called?
  SCRIPT_NAME = SnowPlow::EmrEtlRunner::SCRIPT_NAME

  class ConfigError < ArgumentError; end

  # Return the configuration loaded from the supplied YAML file, plus
  # the additional constants above.
  def Config.get_config()

    options = Config.parse_args()
    config = YAML.load_file(options[:config])

    # Add in the start and end dates
    config[:start] = options[:start]
    config[:end] = options[:end]

    # Add trailing slashes if needed to the buckets
    trail = lambda {|str| return str[-1].chr != '/' ? str << '/' : str}
    config[:s3][:buckets].update(config[:s3][:buckets]){|k,v| trail.call(v)}

    # Construct paths to our HiveQL and serde
    asset_path = "%shive" % config[:s3][:buckets][:assets]
    config[:serde_asset] = "%s/serdes/snowplow-log-deserializers-%s.jar" % [asset_path, config[:snowplow][:serde_version]]
    config[:hiveql_asset] = "%s/hiveql/hive-rolling-etl-%s.q" % [asset_path, config[:snowplow][:hiveql_version]]

    config
  end

  # Parse the command-line arguments
  # Returns: the hash of parsed options
  def Config.parse_args()

    # Handle command-line arguments
    options = {}
    optparse = OptionParser.new do |opts|

      opts.banner = "Usage: %s [options]" % SCRIPT_NAME
      opts.separator ""
      opts.separator "Specific options:"

      opts.on('-c', '--config CONFIG', 'configuration file') { |config| options[:config] = config }
      opts.on('-s', '--start YYYY-MM-DD', 'optional start date *') { |config| options[:start] = config }
      opts.on('-e', '--end YYYY-MM-DD', 'optional end date *') { |config| options[:end] = config }

      opts.separator ""
      opts.separator "* filters the raw event logs processed by EmrEtlRunner by their timestamp"

      opts.separator ""
      opts.separator "Common options:"

      opts.on_tail('-h', '--help', 'Show this message') { puts opts; exit }
      opts.on_tail('-v', "--version", "Show version") do
        puts "%s %s" % [SCRIPT_NAME, SnowPlow::EmrEtlRunner::VERSION]
        exit
      end
    end

    # Run OptionParser's structural validation
    begin
      optparse.parse!
    rescue OptionParser::InvalidOption, OptionParser::MissingArgument
      raise ConfigError, "#{$!.to_s}\n#{optparse}"
    end

    # Check we have a config file argument
    if options[:config].nil?
      raise ConfigError, "Missing option: config\n#{optparse}"
    end

    # Check the config file exists
    unless File.file?(options[:config])
      raise ConfigError, "Configuration file '#{options[:config]}' does not exist, or is not a file."
    end

    # Finally check that start is before end, if both set
    if !options[:start].nil? and !options[:end].nil?
      if options[:start] > options[:end]
        raise ConfigError, "Invalid options: end date #{options[:end]} is before start date #{options[:start]}"
      end
    end

    options
  end

end
#!/usr/bin/env ruby

require 'optparse'

class IPGenerator
  def initialize(session_count, session_length)
    @session_count = session_count
    @session_length = session_length
    @sessions = {}
  end

  def get_ip
    session_gc
    session_create

    ip = @sessions.keys[Kernel.rand(@sessions.length)]
    @sessions[ip] += 1
    return ip
  end

  private

  def session_create
    while @sessions.length < @session_count
      @sessions[random_ip] = 0
    end
  end

  def session_gc
    @sessions.each do |ip, count|
      @sessions.delete(ip) if count >= @session_length
    end
  end

  def random_ip
    octets = []
    octets << Kernel.rand(223) + 1
    3.times { octets << Kernel.rand(255) }

    return octets.join(".")
  end
end

class LogGenerator
  EXTENSIONS = {
    'html' => 40,
    'php' => 30,
    'png' => 15,
    'gif' => 10,
    'css' => 5,
  }

  RESPONSE_CODES = {
    200 => 92,
    404 => 5,
    503 => 3,
  }

  USER_AGENTS = {
    "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; .NET CLR 1.1.4322)" => 30,
    "Mozilla/5.0 (X11; Linux i686) AppleWebKit/534.24 (KHTML, like Gecko) Chrome/11.0.696.50 Safari/534.24" => 30,
    "Mozilla/5.0 (X11; Linux x86_64; rv:6.0a1) Gecko/20110421 Firefox/6.0a1" => 40,
  }

  def initialize(ipgen)
    @ipgen = ipgen
  end

  def write_qps(dest, sleep_secs)
    # sleep = 1.0 / qps
    sleep = sleep_secs
    loop do
      write(dest, 1)
      sleep(sleep)
    end
  end

  def write(dest, count)
    count.times do
      ip = @ipgen.get_ip
      ext = pick_weighted_key(EXTENSIONS)
      resp_code = pick_weighted_key(RESPONSE_CODES)
      resp_size = Kernel.rand(2 * 1024) + 192;
      ua = pick_weighted_key(USER_AGENTS)
      date = Time.now.strftime("%d/%b/%Y:%H:%M:%S %z")
      dest.write("#{ip} - - [#{date}] \"GET /test.#{ext} HTTP/1.1\" " +
                 "#{resp_code} #{resp_size} \"-\" \"#{ua}\"\n")
    end
  end

  private
  def pick_weighted_key(hash)
    total = 0
    hash.values.each { |t| total += t }
    random = Kernel.rand(total)

    running = 0
    hash.each do |key, weight|
      if random >= running and random < (running + weight)
        return key
      end
      running += weight
    end

    return hash.keys.first
  end
end

options = {}
optparse = OptionParser.new do |opts|
  opts.banner = "Usage: #{$PROGRAM_NAME} -s <sleep time> -c <output to console> -f <output file path>"
  opts.on( '-c', '--console', 'Output to stdout') do
    options[:console] = true
  end
  opts.on('-s', '--sleep <sleep time>', 'Sleep time between messages in seconds') do |secs|
    options[:sleep] = secs
  end
  opts.on('-f', '--file <file path>', 'File path where to write log messages') do |path|
    options[:file] = path
    options[:console] = false
  end
  opts.on('-h', '--help', 'Display this screen') do
    puts opts
    puts "Usage Ex: Generate random apache log data to file and sleep 100 ms between messages"
    puts "#{$PROGRAM_NAME} -s 0.1 -f /tmp/apache.log"
    exit
  end
end

begin
  optparse.parse!
rescue OptionParser::InvalidOption, OptionParser::MissingArgument
  puts $!.to_s
  puts optparse
  exit
end

default_opts = {
  :console => true,
  :sleep => 0.1,
  :file => '/tmp/apache.log'
}.merge(options)

output_dest = if default_opts[:console]
                $stdout
              else
                default_opts[:file]
              end

p default_opts
puts "[DEBUG]: Writing output to '#{output_dest}' with sleep time of '#{default_opts[:sleep]}' seconds in between messgaes"
unless default_opts[:console]
  unless File.exists?(File.expand_path(output_dest))
    require 'fileutils'
    puts "[DEBUG]: Specified file does not exist creating file #{output_dest}"
    FileUtils.touch(output_dest)
  end
end

#Start generating data
begin
  output_dest = File.open(default_opts[:file], 'w') unless default_opts[:console]
  puts "[DEBUG]: Init data generator, press ^C to exit"
  output_dest.sync = true
  LogGenerator.new(IPGenerator.new(100, 10)).write_qps(output_dest, default_opts[:sleep].to_i)
  output_dest.close unless default_opts[:console]
rescue Interrupt
  puts "Caught interrupt, aborting!"
ensure
  output_dest.close unless default_opts[:console]
end
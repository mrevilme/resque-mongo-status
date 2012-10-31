dir = File.dirname(File.expand_path(__FILE__))
$LOAD_PATH.unshift dir + '/../lib'
$TESTING = true
require 'test/unit'
require 'rubygems'
require 'shoulda'
require 'mocha'

require 'resque-status'

class Test::Unit::TestCase
end

#
# make sure we can run redis
#

if !system("which mongod")
  puts '', "** can't find `mongod` in your path"
  abort ''
end
#
# start our own mongod when the tests start,
# kill it when they end
#

at_exit do
  next if $!

  if defined?(MiniTest)
    exit_code = MiniTest::Unit.new.run(ARGV)
  else
    exit_code = Test::Unit::AutoRunner.run
  end

  pid = `ps -e -o pid,command | grep "mongod.*test"`.split(" ")[0]
  puts "Killing test mongod server..."
  Process.kill("KILL", pid.to_i)
  `rm -rf #{dir}/db/`
  `mkdir #{dir}/db`
  exit exit_code
end

puts "Starting mongod for testing at localhost:7944..."
`mongod -f #{dir}/mongod.conf --dbpath #{dir}/db/ --logpath #{dir}/db.log`
Resque.mongo = 'localhost:7944/1'

#### Fixtures

class WorkingJob

  include Resque::Plugins::Status

  def perform
    total = options['num']
    (1..total).each do |num|
      at(num, total, "At #{num}")
    end
  end

end

class ErrorJob

  include Resque::Plugins::Status

  def perform
    raise "I'm a bad little job"
  end

end

class KillableJob

  include Resque::Plugins::Status

  def perform
    Resque.redis.set("#{uuid}:iterations", 0)
    100.times do |num|
      Resque.redis.incr("#{uuid}:iterations")
      at(num, 100, "At #{num} of 100")
    end
  end

end

class BasicJob
  include Resque::Plugins::Status
end

class FailureJob
  include Resque::Plugins::Status

  def perform
    failed("I'm such a failure")
  end
end

class NeverQueuedJob
  include Resque::Plugins::Status

  def self.before_enqueue(*args)
    false
  end

  def perform
    # will never get called
  end
end

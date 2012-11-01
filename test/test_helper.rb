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
  puts "Killing test mongod server... #{pid}"
  Process.kill("KILL", pid.to_i)
  FileUtils.rm_rf "#{File.dirname(File.expand_path(__FILE__))}/db/"
  FileUtils.mkdir_p "#{File.dirname(File.expand_path(__FILE__))}/db"
  exit exit_code
end

puts "Starting mongod for testing at localhost:7944..."
`mongod -f #{File.dirname(File.expand_path(__FILE__))}/mongod.conf --dbpath #{File.dirname(File.expand_path(__FILE__))}/db/ --logpath #{File.dirname(File.expand_path(__FILE__))}/db.log`
mongo_starting = true
while mongo_starting
  begin 
    Resque.mongo = Mongo::Connection.new('localhost',7944).db
    mongo_starting = false
  rescue Mongo::ConnectionFailure 
    sleep 1
  end
end

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
    # Resque.redis.set("#{uuid}:iterations", 0)
    Resque.mongo['iterations'].find_one({:uuid => uuid})
    Resque.mongo['iterations'] << {:uuid => uuid, :step => 0}
    100.times do |num|
      Resque.mongo['iterations'].find_and_modify(
        :query => {:uuid => uuid}, 
        :sort => [[:"_id", Mongo::ASCENDING]],
        :update => {"$inc" => {:step => 1 }}
      )
      # Resque.redis.incr("#{uuid}:iterations")
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

def iteration_step (uuid)
  Resque.mongo['iterations'].find_one({:uuid => uuid}, {:fields => [:step]})['step'].to_i
end

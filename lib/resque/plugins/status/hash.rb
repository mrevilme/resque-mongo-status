require 'securerandom'

module Resque
  module Plugins
    module Status

      # Resque::Plugins::Status::Hash is a Hash object that has helper methods for dealing with
      # the common status attributes. It also has a number of class methods for
      # creating/updating/retrieving status objects from Mongo
      class Hash < ::Hash

        extend Resque::Helpers

        # Create a status, generating a new UUID, passing the message to the status
        # Returns the UUID of the new status.
        def self.create(uuid, *messages)
          set(uuid, *messages)

          mongo_statuses.find({:time => {"$lte" => Time.now.to_i - @expire_in}}, {:fields => [:uuid]}).each do |status|
            remove(status['uuid'])
          end if @expire_in
          
          uuid
        end

        # Get a status by UUID. Returns a Resque::Plugins::Status::Hash
        def self.get(uuid)
          val = mongo_statuses.find_one({:uuid => uuid}, { :fields => {:_id => 0} })
          val ? Resque::Plugins::Status::Hash.new(uuid, val.to_hash) : nil
        end

        # Get multiple statuses by UUID. Returns array of Resque::Plugins::Status::Hash
        def self.mget(uuids)
          uuids.map do |uuid|
            if item = mongo_statuses.find_one({'uuid' => uuid})
              Resque::Plugins::Status::Hash.new(item['uuid'], item.to_hash)
            else
              nil
            end
          end
        end

        # set a status by UUID. <tt>messages</tt> can be any number of strings or hashes
        # that are merged in order to create a single status.
        def self.set(uuid, *messages)
          val = Resque::Plugins::Status::Hash.new(uuid, *messages)
          if obj = Resque::mongo_statuses.find_one({:uuid => uuid})
            Resque::mongo_statuses.update({ :uuid => uuid }, {"$set" => val.to_hash})
          else
            Resque::mongo_statuses << val.to_hash
          end
          val
        end

        # clear statuses from mongo passing an optional range. See `statuses` for info
        # about ranges
        def self.clear(range_start = nil, range_end = nil)
          status_ids(range_start, range_end).each do |id|
            remove(id)
          end
        end

        def self.clear_completed(range_start = nil, range_end = nil)
          status_ids(range_start, range_end).select do |id|
            get(id).completed?
          end.each do |id|
            remove(id)
          end
        end

        def self.clear_failed(range_start = nil, range_end = nil)
          status_ids(range_start, range_end).select do |id|
            get(id).failed?
          end.each do |id|
            remove(id)
          end
        end

        def self.remove(uuid)

          mongo_statuses.find_and_modify(
            :query => {:uuid => uuid}, 
            :remove => true,
            :sort => [[:uuid, :asc]]
          )
        end

        def self.count
          mongo_statuses.find({}).count
        end

        # Return <tt>num</tt> Resque::Plugins::Status::Hash objects in reverse chronological order.
        # By default returns the entire set.
        # @param [Numeric] range_start The optional starting range
        # @param [Numeric] range_end The optional ending range
        # @example retuning the last 20 statuses
        #   Resque::Plugins::Status::Hash.statuses(0, 20)
        def self.statuses(range_start = nil, range_end = nil)
          status_ids(range_start, range_end).collect do |id|
            get(id) if id
          end.compact
        end

        # Return the <tt>num</tt> most recent status/job UUIDs in reverse chronological order.
        def self.status_ids(range_start = nil, range_end = nil)
          #   # Because we want a reverse chronological order, we need to get a range starting
          #   # by the higest negative number.
          #   # perspective so we need to convert the passed params
            opts = {
              :sort => [['_id', Mongo::DESCENDING]], 
              :fields => ['uuid'],
              :skip => (range_start and range_end) ? range_start : 0 || 0,
              :limit => (range_end and range_start) ? ((range_end - range_start) < 1) ? 1 : (range_end - range_start) : 0
            }

            mongo_statuses.find({}, opts).map { |d| d['uuid'] } || []

          # end
        end

        # Kill the job at UUID on its next iteration this works by adding the UUID to a
        # kill list (a.k.a. a list of jobs to be killed. Each iteration the job checks
        # if it _should_ be killed by calling <tt>tick</tt> or <tt>at</tt>. If so, it raises
        # a <tt>Resque::Plugins::Status::Killed</tt> error and sets the status to 'killed'.
        def self.kill(uuid)
          mongo_statuses.update({:uuid => uuid}, {"$set" => { kill_key.to_sym => true } })
        end

        # Remove the job at UUID from the kill list
        def self.killed(uuid)
          mongo_statuses.update({:uuid => uuid}, {"$set" => {kill_key.to_sym => false}})
        end

        # Return the UUIDs of the jobs on the kill list
        def self.kill_ids
          mongo_statuses.find({kill_key => true}, {:fields => ['uuid']}).map { |d| d['uuid']} || []
        end

        # Kills <tt>num</tt> jobs within range starting with the most recent first.
        # By default kills all jobs.
        # Note that the same conditions apply as <tt>kill</tt>, i.e. only jobs that check
        # on each iteration by calling <tt>tick</tt> or <tt>at</tt> are eligible to killed.
        # @param [Numeric] range_start The optional starting range
        # @param [Numeric] range_end The optional ending range
        # @example killing the last 20 submitted jobs
        #   Resque::Plugins::Status::Hash.killall(0, 20)
        def self.killall(range_start = nil, range_end = nil)
          status_ids(range_start, range_end).collect do |id|
            kill(id)
          end
        end

        # Check whether a job with UUID is on the kill list
        def self.should_kill?(uuid)
          mongo_statuses.find_one({:uuid => uuid, kill_key => true})
        end

        # The time in seconds that jobs and statuses should expire from Mongo (after
        # the last time they are touched/updated)
        def self.expire_in
          @expire_in
        end

        # Set the <tt>expire_in</tt> time in seconds
        def self.expire_in=(seconds)
          @expire_in = seconds.nil? ? nil : seconds.to_i
        end

        def self.status_key(uuid)
          "status:#{uuid}"
        end

        def self.set_key
          "_statuses"
        end

        def self.kill_key
          "_kill"
        end

        def self.generate_uuid
          SecureRandom.hex.to_s
        end

        def self.hash_accessor(name, options = {})
          options[:default] ||= nil
          coerce = options[:coerce] ? ".#{options[:coerce]}" : ""

          module_eval <<-EOT
          def #{name}
            value = (self['#{name}'] ? self['#{name}']#{coerce} : #{options[:default].inspect})
            yield value if block_given?
            value
          end

          def #{name}=(value)
            self['#{name}'] = value
          end

          def #{name}?
            !!self['#{name}']
          end
          EOT
        end

        STATUSES = %w{queued working completed failed killed}.freeze

        hash_accessor :uuid
        hash_accessor :name
        hash_accessor :status
        hash_accessor :message
        hash_accessor :time
        hash_accessor :options

        hash_accessor :num
        hash_accessor :total

        # Create a new Resque::Plugins::Status::Hash object. If multiple arguments are passed
        # it is assumed the first argument is the UUID and the rest are status objects.
        # All arguments are subsequentily merged in order. Strings are assumed to
        # be messages.
        def initialize(*args)
          super nil
          base_status = {
            'time' => Time.now.to_i,
            'status' => 'queued'
          }
          base_status['uuid'] = args.shift if args.length >= 1
          status_hash = args.inject(base_status) do |final, m|
            m = {'message' => m} if m.is_a?(String)
            final.merge(m || {})
          end
          self.replace(status_hash)
        end

        # calculate the % completion of the job based on <tt>status</tt>, <tt>num</tt>
        # and <tt>total</tt>
        def pct_complete
          case status
          when 'completed' then 100
          when 'queued' then 0
          else
            t = (total == 0 || total.nil?) ? 1 : total
            (((num || 0).to_f / t.to_f) * 100).to_i
          end
        end

        # Return the time of the status initialization. If set returns a <tt>Time</tt>
        # object, otherwise returns nil
        def time
          time? ? Time.at(self['time']) : nil
        end

        STATUSES.each do |status|
          define_method("#{status}?") do
            self['status'] === status
          end
        end

        # Can the job be killed? 'failed', 'completed', and 'killed' jobs cant be killed
        # (for pretty obvious reasons)
        def killable?
          !['failed', 'completed', 'killed'].include?(self.status)
        end

        unless method_defined?(:to_json)
          def to_json(*args)
            json
          end
        end

        # unless method_defined?(:to_hash)
          def to_hash
            self.dup.tap do |h|
              h['pct_complete'] = pct_complete
            end
          end
        # end

        # Return a JSON representation of the current object.
        def json
          
          self.class.encode(hash)
        end

        def inspect
          "#<Resque::Plugins::Status::Hash #{super}>"
        end

      end
    end
  end
end

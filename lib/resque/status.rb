require 'resque'

module Resque
  autoload :JobWithStatus, "#{File.dirname(__FILE__)}/job_with_status"
  module Plugins
    autoload :Status, "#{File.dirname(__FILE__)}/plugins/status"
  end
end

Resque.module_eval <<-EOT
def mongo_statuses
  unless @statuses_index
  	mongo['resque.statuses'].ensure_index( [['time',Mongo::ASCENDING]], { expireAfterSeconds: 3600 } )
	mongo['resque.statuses'].create_index( [['uuid',Mongo::ASCENDING]], { :unique => true })
	@statuses_index = true
  end
  mongo['resque.statuses']
end
EOT

Resque::Helpers.module_eval <<-EOT
def mongo_statuses
  Resque.mongo_statuses
end
EOT
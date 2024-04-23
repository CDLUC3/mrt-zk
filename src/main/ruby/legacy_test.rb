# test the ability to read legacy ingest queue ZK nodes
require_relative "lib/merritt_zk"
require 'zk'

puts "testing mrt-zk gem load"
# run the following from the target env
# export ZKCONN=`get_ssm_value_by_name inventory/zoo/queueService`
zk = ZK.new(ENV.fetch("ZKCONN", "localhost:8084"))
MerrittZK::LegacyIngestJob.list_jobs(zk).each do |j|
  puts "#{j.fetch('id', '')}: #{j.fetch('title', '')} #{j.fetch('status', '')}\n\t#{j.fetch('date', '')} "
end
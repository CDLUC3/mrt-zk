# test the ability to read legacy ingest queue ZK nodes
require_relative "lib/merritt_zk"
require 'zk'

# run the following from the target env
# export ZKCONN=`get_ssm_value_by_name inventory/zoo/queueService`
zk = ZK.new(ENV.fetch("ZKCONN", "localhost:8084"))
p = "/accessSmall.1"
# p = "/accessLarge.1"
# p = "/mrt.inventory.full"
# p = "/mrt.InvLock"
# p = "/mrt.lock"
# p = "/ingest"
zk.children(p).each do |cp|
  puts cp
  d = zk.get("#{p}/#{cp}")
  puts d
end
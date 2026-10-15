# frozen_string_literal: true

# test the ability to read legacy ingest queue ZK nodes
require_relative 'lib/merritt_zk'
require 'zk'

zk = ZK.new(ENV.fetch('ZKCONN', 'localhost:8084'))
MerrittZK::Batch.list_batches_as_json(zk).each do |b|
  puts JSON.pretty_generate(b)
end

# frozen_string_literal: true

# test the ability to read legacy ingest queue ZK nodes
require_relative 'lib/merritt_zk'
require 'zk'

def get_payload(p, d)
  %w[/ingest /accessLarge.1 /accessSmall.1 /mrt.inventory.full].include?(p)
  %w[/mrt.lock /mrt.InvLock].include?(p)
  if %w[/ingest].include?(p)
    JSON.parse(d.bytes[9..].pack('c*'))
  elsif %w[/accessLarge.1 /accessSmall.1 /mrt.inventory.full].include?(p)
    d.bytes[9..].pack('c*')
  elsif %w[/mrt.lock /mrt.InvLock].include?(p)
    d.bytes[8..].pack('c*')
  else
    begin
      return JSON.parse(d, symbolize_names: true)
    rescue StandardError
      # no action
    end
    d
  end
end

def show_node(zk, p, cpath)
  puts cpath
  d = get_payload(p, zk.get(cpath)[0])
  if d.is_a?(Hash)
    puts JSON.pretty_generate(d)
  else
    puts d
  end
  puts

  zk.children(cpath).each do |ccp|
    show_node(zk, p, "#{cpath}/#{ccp}")
  end
end

def show(zk, arr)
  arr.each do |p|
    next unless zk.exists?(p)

    puts '---------'
    zk.children(p).each do |cp|
      show_node(zk, p, "#{p}/#{cp}")
    end
  end
end

# run the following from the target env
# export ZKCONN=`get_ssm_value_by_name inventory/zoo/queueService`
zk = ZK.new(ENV.fetch('ZKCONN', 'localhost:8084'))
LIST = %w[
  /batches /batch-uuids /jobs /jobs/states /access /access/small /access/large
  /locks /locks/queue /locks/storage /locks/inventory /locks/collection
  /migration /migration/m1
].freeze

puts '===> LEGACY'

show(zk, %w[/accessSmall.1 /accessLarge.1 /mrt.inventory.full /mrt.InvLock /mrt.lock /ingest])

if ARGV.include?('-migrate')
  LIST.each do |p|
    zk.rm_rf(p)
    zk.create(p, data: nil)
  end

  batches = {}
  MerrittZK::LegacyIngestJob.list_jobs(zk).each do |j|
    buuid = j.fetch(:batchID, '')
    b = batches[buuid]
    if b.nil?
      batch = MerrittZK::Batch.create_batch(zk, { migrated: true, batchID: buuid })
      b = batch.id
      batches[buuid] = b
    end
    job = MerrittZK::Job.create_job(zk, b, j)
    status = j.fetch('status', '')
    case status
    when 'Pending'
      # no action
    when 'Consumed'
      job.set_status(zk, MerrittZK::JobState::Estimating)
    when 'Completed'
      job.set_status(zk, MerrittZK::JobState::Estimating)
      job.set_status(zk, MerrittZK::JobState::Provisioning)
      job.set_status(zk, MerrittZK::JobState::Downloading)
      job.set_status(zk, MerrittZK::JobState::Processing)
      job.set_status(zk, MerrittZK::JobState::Recording)
      job.set_status(zk, MerrittZK::JobState::Notify)
      job.set_status(zk, MerrittZK::JobState::Completed)
    when 'Failed'
      job.set_status(zk, MerrittZK::JobState::Estimating)
      job.set_status(zk, MerrittZK::JobState::Failed)
    when 'Deleted'
      job.set_status(zk, MerrittZK::JobState::Held)
      job.set_status(zk, MerrittZK::JobState::Deleted)
    when 'Held'
      job.set_status(zk, MerrittZK::JobState::Held)
    end
  end

  MerrittZK::LegacyAccessJob.list_jobs(zk).each do |j|
    if j.fetch(:queueNode, '') == MerrittZK::LargeLegacyAccessJob::DIR
      MerrittZK::Access.create_assembly(zk, MerrittZK::Access::LARGE, j)
    else
      MerrittZK::Access.create_assembly(zk, MerrittZK::Access::SMALL, j)
    end
  end
end

if ARGV.include?('-debug')
  puts '===> DEBUG'
  MerrittZK::LegacyIngestJob.list_jobs(zk).each do |j|
    puts j.fetch(:path, '')
    puts JSON.pretty_generate(j)
  end

  MerrittZK::LegacyInventoryJob.list_jobs(zk).each do |j|
    puts j.fetch(:path, '')
    puts JSON.pretty_generate(j)
  end

  MerrittZK::LegacyAccessJob.list_jobs(zk).each do |j|
    puts j.fetch(:path, '')
    puts JSON.pretty_generate(j)
  end

  MerrittZK::Access.list_jobs(zk).each do |j|
    puts j.fetch(:path, '')
    puts JSON.pretty_generate(j)
  end
end

if ARGV.include?('-clear')
  LIST.each do |p|
    zk.rm_rf(p)
  end
end

if ARGV.include?('-m1')
  LIST.each do |_p|
    zk.rm_rf('/migration')
    zk.create('/migration', data: nil)
    zk.create('/migration/m1', data: nil)
  end
end

if ARGV.include?('-m13')
  LIST.each do |_p|
    zk.rm_rf('/migration')
    zk.create('/migration', data: nil)
    zk.create('/migration/m1', data: nil)
    zk.create('/migration/m3', data: nil)
  end
end

if ARGV.include?('-m0')
  LIST.each do |_p|
    zk.rm_rf('/migration')
  end
end

puts '===> MIGRATED'

show(zk, %w[/batches /batch-uuids /jobs /locks /access /migration])

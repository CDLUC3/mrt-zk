# Merritt Ingest Queue Library (2024 Refactoring)

This microservice is part of the [Merritt Preservation System](https://github.com/CDLUC3/mrt-doc). 

## Purpose

ZooKeeper API for Merritt Microservices.
- Match ingest workload to available resources (compute, memory, working storage)
  - dynamically provision resources to match demand
  - dynamically manage running thread count based on processing load
- Hold jobs based on temporary holds (collection lock, storage node lock, queue hold)
- Graceful resumption of processing in progress
  - allow processing to be resumed on a different ingest host
- Accurate notification of ingest completion (including inventory recording)
  - send accurate summary email on completion of a batch regardless of any interruption that occurred while processing

## API Documentation
- [make_api.sh](make_api.sh)
- [Java API](https://cdluc3.github.io/mrt-zk/api/java/)
- [Ruby API](https://cdluc3.github.io/mrt-zk/api/ruby/)

## Design documents
- [Queue State Transitions](design/states.md)
- [Queue Entry Data Storage](design/data.md)
- [State Transition Details](design/transition.md)
- [Admin Function Mapping](design/queue-admin.md)
- [Use Cases](design/use-cases.md)

## Start a local ZK instance (for integration tests)

```
docker-compose up -d
```

## Java Code -- Purpose

Ensure that the following enums implement the state transitions defined in [states.yml](states.yml)
- [BatchState](src/main/java/org/cdlib/mrt/zk/BatchState.java)
- [JobState](src/main/java/org/cdlib/mrt/zk/JobState.java)

## Java Unit Tests

- [State Transition Tests](src/test/java/org/cdlib/mrt/zk/StateTest.java)
- [ZooKeeper Node Tests](src/test/java/org/cdlib/mrt/zk/ZKTestTest.java)

```
mvn clean install
```

## Publish Javadocs

```
mvn clean javadoc:javadoc
```

## Ruby Code 

### State Transition Classes - Like the java Enums but uses the yaml file directly
- [merritt_zk.rb](src/main/ruby/merritt_zk.rb#L6-L125)

### State Transition Unit Tests
- [spec/state_spec.rb](src/main/ruby/spec/states_spec.rb)

```
cd src/main/ruby
bundle install
bundle exec rspec spec/states_spec.rb
```

### ZK Queue API Library (under construction)
- [merritt_zk.rb](src/main/ruby/merritt_zk.rb#L127-L381)

### ZK Queue API Integration Tests (requires a running zk on localhost - under construction)
- [spec/zk_spec.rb](src/main/ruby/spec/zk_spec.rb)

```
cd src/main/ruby
bundle install
bundle exec rspec spec/zk_spec.rb
```

### Generate ruby docs
```
cd src/main/ruby
bundle exec rdoc merritt_zk.rb 
```

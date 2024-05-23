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
- [Java API](https://cdluc3.github.io/mrt-zk/api/java/)
- [Ruby API](https://cdluc3.github.io/mrt-zk/api/ruby/)

## Design documents
- [Queue State Transitions](design/states.md)
- [Queue Entry Data Storage](design/data.md)
- [State Transition Details](design/transition.md)
- [Admin Function Mapping](design/queue-admin.md)
- [Use Cases](design/use-cases.md)

## Code Build

### Java

```bash
maven clean install
```

### Ruby

First Time
```bash
cd src/main/ruby
bundle install
```

Subsequent Updates
```bash
cd src/main/ruby
bundle update
```

## Code Test

The mrt-zk library contains a small number of Unit Tests.

The majority of mrt-zk tests require a running instance of ZooKeeper.

Therefore, these instructions will show how to run both unit tests and integration tests.

### Java maven

Maven will start/stop an integration test instance of ZooKeeper as tests are executed.

```
maven clean install
```

### Java Tests - Manual Container Start

To make sure that the jar is up to date, build without running tests
```
mvn install -Ddocker.skip -DskipITs -Dmaven.test.skip=true
```

Launch Containers
```
docker-compose up -d
```

Run the junit tests in VSCode.

Stop the contaienr
```
docker-compose down
```

### Ruby Code 

```
cd src/main/ruby
bundle install
bundle exec rspec spec/states_spec.rb
```


## Code Lint

### Ruby Linting

This check is also enforced via GitHub actions
```
cd src/main/ruby
bundle exec rubocop
```

### Java Linting

No linting exists for our Java code

## Update API Docs

The following script [make_api.sh](make_api.sh) will build both javadocs and rubydocs.

Currently, the published API docs are checked into GitHub.

Eventually, we plan to publish these separately from GitHub.

```bash
make_api.sh
```
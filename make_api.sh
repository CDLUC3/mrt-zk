#!/bin/sh
mvn clean javadoc:javadoc
cd src/main/ruby
bundle exec rdoc merritt_zk.rb
cd ../../..
rm -rf api
mkdir -p api/java api/ruby
cp -r target/site/apidocs/* api/java
cp -r src/main/ruby/doc/* api/ruby

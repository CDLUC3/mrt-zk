#!/bin/sh
mvn clean javadoc:javadoc
cd src/main/ruby
rm -rf doc
bundle exec rdoc lib/*.rb
cd ../../..
rm -rf api
mkdir -p api/java api/ruby
cp -r target/site/apidocs/* api/java
cp -r src/main/ruby/doc/* api/ruby

# derbydump


Derby SQL dump. This project will take a Derby database and export it to a file, suitable for importing to mysql (or probabably some other SQL compliance database).

## Building

Clone the reposiory, then execute 'mvn package' in the main directory.

## Setup

Please create a file called 'dump.properties' in the folder the application will be run from.
See sample.dump.properties for more information.

## Running

Run application with 'java -jar [pathTo]/exporter-0.0.1-SNAPSHOT-jar-with-dependencies.jar'.


## Continuous integration testing

[![Build Status](https://travis-ci.org/ari/derbydump.png?branch=master)](https://travis-ci.org/ari/derbydump)



## License

    Copyright 2013 ish group pty ltd

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.

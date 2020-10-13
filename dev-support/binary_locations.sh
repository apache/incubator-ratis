# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# List of binary artifacts used by the Docker and Vagrant test code
# Provides a central point of URLs and checksums for all builds

# Maven Location
export ratis_maven_file="apache-maven-3.6.2-bin.tar.gz"
export ratis_maven_url="https://archive.apache.org/dist/maven/maven-3/3.6.2/binaries/${ratis_maven_file}"
export ratis_maven_sum="d941423d115cd021514bfd06c453658b1b3e39e6240969caf4315ab7119a77299713f14b620fb2571a264f8dff2473d8af3cb47b05acf0036fc2553199a5c1ee"

# Go Location
export ratis_go_file="go1.13.linux-amd64.tar.gz"
export ratis_go_url="https://dl.google.com/go/${ratis_go_file}"
export ratis_go_sum="68a2297eb099d1a76097905a2ce334e3155004ec08cdea85f24527be3c48e856"

# ProtoC Location
export ratis_protoc_file="protoc-3.5.0-linux-x86_64.zip"
export ratis_protoc_url="https://github.com/google/protobuf/releases/download/v3.5.0/${ratis_protoc_file}"
export ratis_protoc_sum="49aa98db1877dcb69e89c7d217bb70cb1678d2266c3172f817348f2b5aab1d6a"

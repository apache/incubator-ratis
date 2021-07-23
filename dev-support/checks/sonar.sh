#!/usr/bin/env bash
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
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
cd "$DIR/../.." || exit 1

if [ ! "$SONAR_TOKEN" ]; then
  echo "SONAR_TOKEN environment variable should be set"
  exit 1
fi
mvn -B verify -DskipShade -DskipTests org.sonarsource.scanner.maven:sonar-maven-plugin:3.6.0.1398:sonar \
-Dsonar.host.url=https://sonarcloud.io \
-Dsonar.coverage.jacoco.xmlReportPaths=$(find "$(pwd)" -path '*jacoco.xml' | tr '\n' ',') \
-Dsonar.organization=apache -Dsonar.projectKey=apache-ratis

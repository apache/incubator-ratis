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

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

#Workspace is set by the jenkins, by default (local run) is the parent directory of the checkout.
WORKSPACE=${WORKSPACE:-$DIR/../../..}
cd $WORKSPACE

YETUSDIR=${WORKSPACE}/yetus
TESTPATCHBIN=${YETUSDIR}/bin/test-patch
ARTIFACTS=${WORKSPACE}/out
BASEDIR=${WORKSPACE}/sourcedir
TOOLS=${WORKSPACE}/tools
rm -rf "${ARTIFACTS}" "${YETUSDIR}"
mkdir -p "${ARTIFACTS}" "${YETUSDIR}" "${TOOLS}"

#It's not on all the branches, so we need to copy it from the checkout out source
cp $BASEDIR/dev-support/yetus-personality.sh $WORKSPACE/
cp $BASEDIR/dev-support/docker/Dockerfile $WORKSPACE/

source $BASEDIR/dev-support/binary_locations.sh

YETUS_VERSION=${YETUS_VERSION:-0.8.0}
echo "Downloading Yetus"
curl -L https://archive.apache.org/dist/yetus/${YETUS_VERSION}/yetus-${YETUS_VERSION}-bin.tar.gz -o yetus.tar.gz
gunzip -c yetus.tar.gz | tar xpf - -C "${YETUSDIR}" --strip-components 1

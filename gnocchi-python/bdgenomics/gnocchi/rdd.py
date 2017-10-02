# Licensed to Big Data Genomics (BDG) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The BDG licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


class CalledVariantDataset(object):

    def __init__(self, jvmDS, sc):
        self._jvmDS = jvmDS
        self.sc = sc

    def get(self):
        return self._jvmDS


class BetterPhenotype(object):

    def __init__(self, bp, sc):
        self._jvmBetterPhenotype = bp
        self.sc = sc

class BetterPhenotypeMap(object):

    def __init__(self, jvmMap, sc, jgs):
        self._jvmMap = jvmMap
        self.sc = sc
        self._jgs = jgs

    def get(self):
        return self._jvmMap

    def getKey(self, k):
        bp = self._jgs.getBetterPhenotypeByKey(self._jvmMap, k)
        return BetterPhenotype(bp, self.sc)

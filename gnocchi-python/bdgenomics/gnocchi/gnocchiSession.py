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

from org.bdgenomics.gnocchi.rdd import CalledVariantDataset, PhenotypeMap

class GnocchiSession(object):
    """
    """

    def __init__(self, sc):
        self._sc = sc
        self._jvm = sc._jvm
        session = sc._jvm.net.fnothaft.gnocchi.sql.GnocchiSession(sc._jsc.sc())
        self.__jgs = sc._jvm.net.fnothaft.gnocchi.api.java.JavaGnocchiSession(session)

    def filterSamples(self, genotypesDataset, mind, ploidy):
        dataset = self.__jgs.filterSamples(genotypesDataset.get(), mind, ploidy)
        return CalledVariantDataset(dataset, self._sc)

    def filterVariants(self, genotypesDataset, geno, maf):
        dataset = self.__jgs.filterVariants(genotypesDataset.get(), geno, maf)
        return CalledVariantDataset(dataset, self._sc)

    def loadGenotypes(self, genotypesPath):
        dataset = self.__jgs.loadGenotypes(genotypesPath)
        return CalledVariantDataset(dataset, self._sc)

    def loadPhenotypes(self, phenotypesPath, primaryID, phenoName, delimited,
                       covarPath=None, covarNames=None):
        bpMap = self.__jgs.loadPhenotypes(phenotypesPath, primaryID, phenoName,
                                          delimited, covarPath, covarNames)

        return PhenotypeMap(bpMap, self._sc, self.__jgs)

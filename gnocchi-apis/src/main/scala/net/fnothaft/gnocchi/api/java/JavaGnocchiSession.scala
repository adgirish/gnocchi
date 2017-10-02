/**
  * Licensed to Big Data Genomics (BDG) under one
  * or more contributor license agreements.  See the NOTICE file
  * distributed with this work for additional information
  * regarding copyright ownership.  The BDG licenses this file
  * to you under the Apache License, Version 2.0 (the
  * "License"); you may not use this file except in compliance
  * with the License.  You may obtain a copy of the License at
  *
  *     http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */

// (TODO) Add boilerplate for Java wrapper onto Gnocchi

package net.fnothaft.gnocchi.api.java

import org.apache.spark.api.java.JavaSparkContext
import org.bdgenomics.adam.rdd.ADAMContext
import net.fnothaft.gnocchi.sql.GnocchiSession

object JavaGnocchiSession {
  // convert to and from java/scala implementation
  implicit def fromGnocchiSession(gs: GnocchiSession): JavaGnocchiSession = new JavaGnocchiSession(gs)
  implicit def toGnocchiSession(jgs: JavaGnocchiSession): GnocchiSession = jgs.gs
}

// (TODO) Write custom Dataset[CalledVariant] in Python file

/**
 * The JavaGnocchiSession provides java-friendly functions on top of GnocchiSession.
 *
 * @param gs The GnocchiSession to wrap.
 */
 class JavaGnocchiSession(val gs: GnocchiSession) extends Serializable {

  /**
   * @return Returns the Gnocchi Spark Context associated with this Java Gnocchi Session.
   */
  def getSparkContext: JavaSparkContext = new JavaSparkContext(gs.sc)

  /**
   * (TODO) Add comments
   */
  def filterSamples(genotypes: Dataset[CalledVariant], mind: java.lang.Double, ploidy: java.lang.Double): Dataset[CalledVariant] = {
    gs.filterSamples(genotypes, mind, ploidy)
  }

  /**
   * (TODO) Add comments
   */
  def filterVariants(genotypes: Dataset[CalledVariant], geno: java.lang.Double, maf: java.lang.Double): Dataset[CalledVariant] = {
    gs.filterVariants(genotypes, geno, maf)
  }

  /**
   * (TODO) Add comments
   */
  def loadGenotypes(genotypesPath: java.lang.String): Dataset[CalledVariant] = {
    gs.loadGenotypes(genotypesPath)
  }

  /**
   * (TODO) Add comments
   */
  def loadPhenotypes(phenotypesPath: java.lang.String,
                     primaryID: java.lang.String,
                     phenoName: java.lang.String,
                     delimiter: java.lang.String,
                     covarPath: Option[String] = None,
                     covarNames: Option[List[String]] = None): Map[String, BetterPhenotype] = {
    gs.loadPhenotypes(phenotypesPath,
                      primaryID,
                      phenoName,
                      delimiter,
                      covarPath,
                      covarNames)
  }

  /**
   * (TODO) Add comments
   */
  def getBetterPhenotypeByKey(betterPhenotypeMap: Map[String, BetterPhenotype], key: java.lang.String): BetterPhenotype = {
    betterPhenotypeMap(key)
  }
}

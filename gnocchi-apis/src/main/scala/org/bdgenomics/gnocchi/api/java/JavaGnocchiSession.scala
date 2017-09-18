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

object JavaGnocchiSession {
  // convert to and from java/scala implementation
  implicit def fromGnocchiSession(gs: GnocchiSession): JavaGnocchiSession = new JavaGnocchiSession(gs)
  implicit def toGnocchiSession(jgs: JavaGnocchiSession): GnocchiSession = jgs.gs
}

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
  def filterSamples(genotypes: Dataset[CalledVariant], mind: Double, ploidy: Double): Dataset[CalledVariant] = {
    return
  }

  /**
   * (TODO) Add comments
   */
  def filterVariants(genotypes: Dataset[CalledVariant], geno: Double, maf: Double): Dataset[CalledVariant] = {
    return
  }

  /**
   * (TODO) Add comments
   */
  def loadGenotypes(genotypesPath: String): Dataset[CalledVariant] = {
    return 
  }

  /**
   * (TODO) Add comments
   */
  def loadPhenotypes(phenotypesPath: String,
                     primaryID: String,
                     phenoName: String,
                     delimiter: String,
                     covarPath: Option[String] = None,
                     covarNames: Option[List[String]] = None): Map[String, BetterPhenotype] = {
    return 
  }

 }

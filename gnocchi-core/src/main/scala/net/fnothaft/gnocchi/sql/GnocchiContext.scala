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
package net.fnothaft.gnocchi.sql

import net.fnothaft.gnocchi.models.GenotypeState
import org.apache.spark.sql.{ Column, DataFrame, Dataset, SQLContext }
import org.apache.spark.sql.functions._
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.formats.avro.Genotype

object GnocchiContext {

  implicit def gcFromSqlContext(sqlContext: SQLContext): GnocchiContext =
    new GnocchiContext(sqlContext)
}

class GnocchiContext private[sql] (@transient sqlContext: SQLContext) extends Serializable {

  import sqlContext.implicits._

  def toGenotypeStateDataset(gtFrame: DataFrame, ploidy: Int): Dataset[GenotypeState] = {
    toGenotypeStateDataFrame(gtFrame, ploidy).as[GenotypeState]
  }

  def toGenotypeStateDataFrame(gtFrame: DataFrame, ploidy: Int, sparse: Boolean = false): DataFrame = {

    val filteredGtFrame = if (sparse) {
      // if we want the sparse representation, we prefilter
      val sparseFilter = (0 until ploidy).map(i => {
        gtFrame("alleles").getItem(i) !== "Ref"
      }).reduce(_ || _)
      gtFrame.filter(sparseFilter)
    } else {
      gtFrame
    }

    // generate expression
    val genotypeState = (0 until ploidy).map(i => {
      val c: Column = when(filteredGtFrame("alleles").getItem(i) === "REF", 1).otherwise(0)
      c
    }).reduce(_ + _)

    val missingGenotypes = (0 until ploidy).map(i => {
      val c: Column = when(filteredGtFrame("alleles").getItem(i) === "NO_CALL", 1).otherwise(0)
      c
    }).reduce(_ + _)

    filteredGtFrame.printSchema()

    filteredGtFrame.select(filteredGtFrame("variant.contigName").as("contigName"),
      filteredGtFrame("variant.start").as("start"),
      filteredGtFrame("variant.end").as("end"),
      filteredGtFrame("variant.referenceAllele").as("ref"),
      filteredGtFrame("variant.alternateAllele").as("alt"),
      filteredGtFrame("sampleId"),
      filteredGtFrame("variant.annotation.alleleCount").as("ancestralAllele"),
      filteredGtFrame("variant.annotation.alleleCount").as("alleleCount"),
      filteredGtFrame("variant.annotation.readDepth").as("readDepth"),
      filteredGtFrame("variant.annotation.forwardReadDepth").as("forwardReadDepth"),
      filteredGtFrame("variant.annotation.reverseReadDepth").as("reverseReadDepth"),
      filteredGtFrame("variant.annotation.referenceReadDepth").as("referenceReadDepth"),
      filteredGtFrame("variant.annotation.referenceForwardReadDepth").as("referenceForwardReadDepth"),
      filteredGtFrame("variant.annotation.referenceReverseReadDepth").as("referenceReverseReadDepth"),
      filteredGtFrame("variant.annotation.alleleFrequency").as("alleleFrequency"),
      filteredGtFrame("variant.annotation.cigar").as("cigar"),
      filteredGtFrame("variant.annotation.dbSnp").as("dbSnp"),
      filteredGtFrame("variant.annotation.hapMap2").as("hapMap2"),
      filteredGtFrame("variant.annotation.hapMap3").as("hapMap3"),
      filteredGtFrame("variant.annotation.validated").as("validated"),
      filteredGtFrame("variant.annotation.thousandGenomes").as("thousandGenomes"),
      filteredGtFrame("variant.annotation.somatic").as("somatic"),
      filteredGtFrame("variant.annotation.transcriptEffects.alternateAllele").as("transcriptEffectsAlternateAllele"),
      filteredGtFrame("variant.annotation.transcriptEffects.effects").as("transcriptEffectsEffects"),
      filteredGtFrame("variant.annotation.transcriptEffects.geneName").as("transcriptEffectsGeneName"),
      filteredGtFrame("variant.annotation.transcriptEffects.geneId").as("transcriptEffectsGeneId"),
      filteredGtFrame("variant.annotation.transcriptEffects.featureType").as("transcriptEffectsFeatureType"),
      filteredGtFrame("variant.annotation.transcriptEffects.featureId").as("transcriptEffectsFeatureId"),
      filteredGtFrame("variant.annotation.transcriptEffects.biotype").as("transcriptEffectsBiotype"),
      filteredGtFrame("variant.annotation.transcriptEffects.rank").as("transcriptEffectsRank"),
      filteredGtFrame("variant.annotation.transcriptEffects.total").as("transcriptEffectsTotal"),
      filteredGtFrame("variant.annotation.transcriptEffects.genomicHgvs").as("transcriptEffectsGenomicHgvs"),
      filteredGtFrame("variant.annotation.transcriptEffects.transcriptHgvs").as("transcriptEffectsTranscriptHgvs"),
      filteredGtFrame("variant.annotation.transcriptEffects.proteinHgvs").as("transcriptEffectsProteinHgvs"),
      filteredGtFrame("variant.annotation.transcriptEffects.cdnaPosition").as("transcriptEffectsCdnaPosition"),
      filteredGtFrame("variant.annotation.transcriptEffects.cdnaLength").as("transcriptEffectsCdnaLength"),
      filteredGtFrame("variant.annotation.transcriptEffects.cdsPosition").as("transcriptEffectsCdsPosition"),
      filteredGtFrame("variant.annotation.transcriptEffects.cdsLength").as("transcriptEffectsCdsLength"),
      filteredGtFrame("variant.annotation.transcriptEffects.proteinPosition").as("transcriptEffectsProteinPosition"),
      filteredGtFrame("variant.annotation.transcriptEffects.proteinLength").as("transcriptEffectsProteinLength"),
      filteredGtFrame("variant.annotation.transcriptEffects.distance").as("transcriptEffectsDistance"),
      filteredGtFrame("variant.annotation.transcriptEffects.messages").as("transcriptEffectsMessages"),
      filteredGtFrame("variant.annotation.attributes").as("attr"),
      genotypeState.as("genotypeState"),
      missingGenotypes.as("missingGenotypes"))

  }
}

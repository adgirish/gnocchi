
package net.fnothaft.gnocchi.cli

import java.nio.file.Files

import net.fnothaft.gnocchi.GnocchiFunSuite
import net.fnothaft.gnocchi.models.GenotypeState
import net.fnothaft.gnocchi.sql.GnocchiContext
import org.apache.commons.io.FileUtils
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{ Dataset, SQLContext }
import org.bdgenomics.adam.cli.Vcf2ADAM
import org.bdgenomics.adam.rdd.ADAMContext
import org.bdgenomics.adam.rdd.ADAMContext._

class AnnotatedVCFHandlingSuite extends GnocchiFunSuite {
  sparkTest("Processing in annotated VCFfile from snpEff") {
    val path = "src/test/resources/testData/AnnotatedVCFHandlingSuite"
    val destination = Files.createTempDirectory("").toAbsolutePath.toString + "/" + path

    val genoFilePath = ClassLoader.getSystemClassLoader.getResource("small_snpeff.vcf").getFile
    val phenoFilePath = ClassLoader.getSystemClassLoader.getResource("2Liner.txt").getFile
    val cliCall = s"../bin/gnocchi-submit regressPhenotypes $genoFilePath $phenoFilePath ADDITIVE_LINEAR $destination -saveAsText -phenoName pheno2 -covar -overwriteParquet"
    val cliArgs = cliCall.split(" ").drop(2)

    val genotypeStateDataset = RegressPhenotypes(cliArgs).loadGenotypes(sc)
    val genotypeStateArray = genotypeStateDataset.collect()

    assert(genotypeStateArray.length == 15)

    for (i <- List.range(0, 15, 3)) {
      println("Row ", i)
      var gs = genotypeStateArray(i)
      println(gs.ancestralAllele)
      println(gs.alleleCount)
      println(gs.readDepth)
      println(gs.forwardReadDepth)
      println(gs.reverseReadDepth)
      println(gs.referenceReadDepth)
      println(gs.referenceForwardReadDepth)
      println(gs.referenceReverseReadDepth)
      println(gs.alleleFrequency)
      println(gs.cigar)
      println(gs.dbSnp)
      println(gs.hapMap2)
      println(gs.hapMap3)
      println(gs.validated)
      println(gs.thousandGenomes)
      println(gs.somatic)
      println(gs.transcriptEffectsAlternateAllele.get.mkString(", "))
      println(gs.transcriptEffectsEffects.get.mkString(", "))
      println(gs.transcriptEffectsGeneName.get.mkString(", "))
      println(gs.transcriptEffectsGeneId.get.mkString(", "))
      println(gs.transcriptEffectsFeatureType.get.mkString(", "))
      println(gs.transcriptEffectsFeatureId.get.mkString(", "))
      println(gs.transcriptEffectsBiotype.get.mkString(", "))
      println(gs.transcriptEffectsRank.get.mkString(", "))
      println(gs.transcriptEffectsTotal.get.mkString(", "))
      println(gs.transcriptEffectsGenomicHgvs.get.mkString(", "))
      println(gs.transcriptEffectsTranscriptHgvs.get.mkString(", "))
      println(gs.transcriptEffectsProteinHgvs.get.mkString(", "))
      println(gs.transcriptEffectsCdnaPosition.get.mkString(", "))
      println(gs.transcriptEffectsCdnaLength.get.mkString(", "))
      println(gs.transcriptEffectsCdsPosition.get.mkString(", "))
      println(gs.transcriptEffectsCdsLength.get.mkString(", "))
      println(gs.transcriptEffectsProteinPosition.get.mkString(", "))
      println(gs.transcriptEffectsProteinLength.get.mkString(", "))
      println(gs.transcriptEffectsDistance.get.mkString(", "))
      println(gs.transcriptEffectsMessages.get.mkString(", "))
      println("-------")
    }

  }
}


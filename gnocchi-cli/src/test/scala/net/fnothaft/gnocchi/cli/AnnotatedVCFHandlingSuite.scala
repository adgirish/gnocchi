
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
import org.bdgenomics.formats.avro.{Genotype, Variant}

class AnnotatedVCFHandlingSuite extends GnocchiFunSuite {
  sparkTest("Processing in annotated VCFfile from snpEff") {
    val path = "src/test/resources/testData/AnnotatedVCFHandlingSuite"
    val destination = Files.createTempDirectory("").toAbsolutePath.toString + "/" + path

    val genoFilePath = ClassLoader.getSystemClassLoader.getResource("small_snpeff.vcf").getFile
    val phenoFilePath = ClassLoader.getSystemClassLoader.getResource("2Liner.txt").getFile
    val cliCall = s"../bin/gnocchi-submit regressPhenotypes $genoFilePath $phenoFilePath ADDITIVE_LINEAR $destination -saveAsText -phenoName pheno2 -covar -overwriteParquet"
    val cliArgs = cliCall.split(" ").drop(2)

    val genotypeStateDataset = RegressPhenotypes(cliArgs).loadGenotypes(sc)
    val genotypeRDD = RegressPhenotypes(cliArgs).loadAnnotations(sc)
    val genotypeStateArray = genotypeStateDataset.collect()

    assert(genotypeStateArray.length == 15)

    for (gt <- genotypeRDD) {
      println(gt.getVariant)
    }
    //    for (i <- 0 to 14 by 3) {
    //      println(genotypeRDD.zipWithIndex.filter(_._2 == i).map(_._1).first())
    //      println()
    //    }
  }
}

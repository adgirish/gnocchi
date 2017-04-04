
package net.fnothaft.gnocchi.cli

import java.nio.file.Files

import net.fnothaft.gnocchi.GnocchiFunSuite
import net.fnothaft.gnocchi.models.GenotypeState
import org.apache.commons.io.FileUtils
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{ Dataset, SQLContext }
import org.bdgenomics.adam.cli.Vcf2ADAM

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

    for (gs <- genotypeStateArray) {
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
      println("-------")
    }
  }
}
//  def run(sc: SparkContext) {
//    val genotypeStates = loadGenotypes(sc)
//  }
//
//  def loadGenotypes(sc: SparkContext): Dataset[GenotypeState] = {
//    // set up sqlContext
//    val sqlContext = SQLContext.getOrCreate(sc)
//
//
//    val vcfPath = testFile("small_snpeff.vcf")
//    val posAndIds = GetVariantIds(sc, vcfPath)
//    //    if (args.getIds) {
//    //      val mapPath = args.mapFile
//    //      val oldName = new File(args.genotypes).getAbsolutePath.split("/").reverse(0)
//    //      val newVCFPath = new File(args.genotypes).getAbsolutePath.split("/").reverse.drop(1).reverse.mkString("/") + "withIds_" + oldName
//    //      val outpath = newVCFPath
//    //      GetVariantIds(sc, vcfPath)
//    //      vcfPath = outpath
//    //    }
//
//    // check for ADAM formatted version of the file specified in genotypes. If it doesn't exist, convert vcf to parquet using vcf2adam.
//    if (!parquetFiles.getAbsoluteFile.exists) {
//      val cmdLine: Array[String] = Array[String](vcfPath, parquetInputDestination)
//      Vcf2ADAM(cmdLine).run(sc)
//    } else if (args.overwrite) {
//      FileUtils.deleteDirectory(parquetFiles)
//      val cmdLine: Array[String] = Array[String](vcfPath, parquetInputDestination)
//      Vcf2ADAM(cmdLine).run(sc)
//    }
//
//    // read in parquet files
//    import sqlContext.implicits._
//    //    val genotypes = sqlContext.read.parquet(parquetInputDestination)
//    val genotypes = sqlContext.read.format("parquet").load(parquetInputDestination)
//    //    val genotypes = sc.loadGenotypes(parquetInputDestination).toDF()
//    // transform the parquet-formatted genotypes into a dataFrame of GenotypeStates and convert to Dataset.
//    val genotypeStates = sqlContext
//      .toGenotypeStateDataFrame(genotypes, args.ploidy, sparse = false)
//    val genoStatesWithNames = genotypeStates.select(concat($"contig", lit("_"), $"end", lit("_"), $"alt") as "contig",
//      genotypeStates("start"),
//      genotypeStates("end"),
//      genotypeStates("ref"),
//      genotypeStates("alt"),
//      genotypeStates("sampleId"),
//      genotypeStates("genotypeState"),
//      genotypeStates("missingGenotypes"))
//
//
//
//    // mind filter
//    genoStatesWithNames.registerTempTable("genotypeStates")
//
//    val mindDF = sqlContext.sql("SELECT sampleId FROM genotypeStates GROUP BY sampleId HAVING SUM(missingGenotypes)/(COUNT(sampleId)*2) <= %s".format(args.mind))
//    // TODO: Resolve with "IN" sql command once spark2.0 is integrated
//    val filteredGenotypeStates = genoStatesWithNames.filter(($"sampleId").isin(mindDF.collect().map(r => r(0)): _*))
//    val postFilter = filteredGenotypeStates.as[GenotypeState].rdd.take(10).toList
//    println("\n\n\n\n\n\n")
//    println(postFilter)
//    println("\n\n\n\n\n\n")
//
//    filteredGenotypeStates.as[GenotypeState]
//  }


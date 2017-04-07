
package net.fnothaft.gnocchi.cli

import java.nio.file.Files

import net.fnothaft.gnocchi.GnocchiFunSuite

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

    val genotypeRDD = RegressPhenotypes(cliArgs).loadAnnotations(sc)

    assert(genotypeStateArray.length === 15)
    assert(genotypeRDD.count === 5)

    assert(genotypeRDD.first._2.getAncestralAllele === null)
    assert(genotypeRDD.first._2.getAlleleCount === 2)
    assert(genotypeRDD.first._2.getReadDepth === null)
    assert(genotypeRDD.first._2.getForwardReadDepth === null)
    assert(genotypeRDD.first._2.getReverseReadDepth === null)
    assert(genotypeRDD.first._2.getReferenceReadDepth === null)
    assert(genotypeRDD.first._2.getReferenceForwardReadDepth === null)
    assert(genotypeRDD.first._2.getReferenceReverseReadDepth === null)
    assert(genotypeRDD.first._2.getAlleleFrequency === 0.333f)
    assert(genotypeRDD.first._2.getCigar === null)
    assert(genotypeRDD.first._2.getDbSnp === null)
    assert(genotypeRDD.first._2.getHapMap2 === null)
    assert(genotypeRDD.first._2.getHapMap3 === null)
    assert(genotypeRDD.first._2.getValidated === null)
    assert(genotypeRDD.first._2.getThousandGenomes === null)
    assert(genotypeRDD.first._2.getSomatic === false)

    //    for (gt <- genotypeRDD) {
    //      println(gt.toString)
    //      println()
    //    }
  }

}


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

    val variantAnnotationRDD = RegressPhenotypes(cliArgs).loadAnnotations(sc)

    assert(genotypeStateArray.length === 15)
    assert(variantAnnotationRDD.count === 5)

    assert(variantAnnotationRDD.first._2.getAncestralAllele === null)
    assert(variantAnnotationRDD.first._2.getAlleleCount === 2)
    assert(variantAnnotationRDD.first._2.getReadDepth === null)
    assert(variantAnnotationRDD.first._2.getForwardReadDepth === null)
    assert(variantAnnotationRDD.first._2.getReverseReadDepth === null)
    assert(variantAnnotationRDD.first._2.getReferenceReadDepth === null)
    assert(variantAnnotationRDD.first._2.getReferenceForwardReadDepth === null)
    assert(variantAnnotationRDD.first._2.getReferenceReverseReadDepth === null)
    assert(variantAnnotationRDD.first._2.getAlleleFrequency === 0.333f)
    assert(variantAnnotationRDD.first._2.getCigar === null)
    assert(variantAnnotationRDD.first._2.getDbSnp === null)
    assert(variantAnnotationRDD.first._2.getHapMap2 === null)
    assert(variantAnnotationRDD.first._2.getHapMap3 === null)
    assert(variantAnnotationRDD.first._2.getValidated === null)
    assert(variantAnnotationRDD.first._2.getThousandGenomes === null)
    assert(variantAnnotationRDD.first._2.getSomatic === false)

    //    for (gt <- variantAnnotationRDD) {
    //      println(gt.toString)
    //      println()
    //    }
  }

  sparkTest("Joining Annotation and Assocation RDDs") {
    val path = "src/test/resources/testData/AnnotatedVCFHandlingSuite"
    val destination = Files.createTempDirectory("").toAbsolutePath.toString + "/" + path

    val genoFilePath = ClassLoader.getSystemClassLoader.getResource("small_snpeff.vcf").getFile
    val phenoFilePath = ClassLoader.getSystemClassLoader.getResource("2Liner.txt").getFile
    val cliCall = s"../bin/gnocchi-submit regressPhenotypes $genoFilePath $phenoFilePath ADDITIVE_LINEAR $destination -saveAsText -phenoName pheno2 -overwriteParquet"
    val cliArgs = cliCall.split(" ").drop(2)

    val genotypeStateDataset = RegressPhenotypes(cliArgs).loadGenotypes(sc)
    val phenotypeStateDataset = RegressPhenotypes(cliArgs).loadPhenotypes(sc)
    val variantAnnotationRDD = RegressPhenotypes(cliArgs).loadAnnotations(sc)

    val associations = RegressPhenotypes(cliArgs).performAnalysis(genotypeStateDataset, phenotypeStateDataset, sc)

    println("HERE!")
    println("COUNT: " + associations.count())
    associations.foreach(println(_))

    assert(variantAnnotationRDD.count === 5)
  }

}

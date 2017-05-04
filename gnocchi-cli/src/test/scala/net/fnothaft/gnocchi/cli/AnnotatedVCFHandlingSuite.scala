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

package net.fnothaft.gnocchi.cli

import java.nio.file.Files
import java.io.File
import net.fnothaft.gnocchi.algorithms.siteregression.AdditiveLinearRegression
import net.fnothaft.gnocchi.sql.GnocchiContext._
import net.fnothaft.gnocchi.sql.GnocchiContext
import org.apache.spark.sql.SparkSession

import scala.io.Source
import org.apache.commons.io.FileUtils
import org.apache.commons.io.filefilter.WildcardFileFilter

import net.fnothaft.gnocchi.GnocchiFunSuite

class AnnotatedVCFHandlingSuite extends GnocchiFunSuite {

  val path = "src/test/resources/testData/Association"
  val destination = Files.createTempDirectory("").toAbsolutePath.toString + "/Association"

  sparkTest("Processing in annotated VCF data from snpEff") {
    val genoFilePath = ClassLoader.getSystemClassLoader.getResource("small_snpeff.vcf").getFile
    val phenoFilePath = ClassLoader.getSystemClassLoader.getResource("2Liner_annot.txt").getFile

    val variantAnnotationRDD = sc.loadAnnotations(genoFilePath, destination, 1, 0.1, 0.1, 0.1, false)

    assert(variantAnnotationRDD.count === 5)

    assert(variantAnnotationRDD.first._2.getAncestralAllele === null)
    assert(variantAnnotationRDD.first._2.getAlleleCount === 2)
    assert(variantAnnotationRDD.first._2.getReadDepth === null)
    assert(variantAnnotationRDD.first._2.getForwardReadDepth === null)
    assert(variantAnnotationRDD.first._2.getReverseReadDepth === null)
    assert(variantAnnotationRDD.first._2.getAlleleFrequency === 0.333f)
    assert(variantAnnotationRDD.first._2.getDbSnp === null)
    assert(variantAnnotationRDD.first._2.getHapMap2 === null)
    assert(variantAnnotationRDD.first._2.getValidated === null)
    assert(variantAnnotationRDD.first._2.getThousandGenomes === null)
    assert(variantAnnotationRDD.first._2.getSomatic === false)
    assert(variantAnnotationRDD.first._2.getAttributes.get("ClippingRankSum") == "-2.196")
  }
  //
  //  sparkTest("Joining Annotation and Assocation RDDs") {
  //    val genoFilePath = ClassLoader.getSystemClassLoader.getResource("small_snpeff.vcf").getFile
  //    val phenoFilePath = ClassLoader.getSystemClassLoader.getResource("2Liner_annot.txt").getFile
  //
  //    val variantAnnotationRDD = gc.loadAnnotations(phenoFilePath, genoFilePath, destination, false) //RegressPhenotypes(cliArgs).loadAnnotations(sc)
  //
  //    val genotypeStates = gc.loadAndFilterGenotypes(genoFilePath, destination, 1, 0.1, 0.1, 0.1, false)
  //    val phenotypes = gc.loadPhenotypes(phenoFilePath, "pheno1", false, false, Option.empty[String], Option.empty[String])
  //    val regressionResult = AdditiveLinearRegression(genotypeStates, phenotypes)
  //
  //    val annotatedAssociations = gc.mergeAdditiveLinearAnnotations(regressionResult, variantAnnotationRDD)
  //
  //    assert(annotatedAssociations.first.variant.getContigName === "1_14400_C")
  //    assert(annotatedAssociations.first.variant.getAlternateAllele === "C")
  //    assert(annotatedAssociations.first.variantAnnotation.isDefined === true)
  //    assert(annotatedAssociations.first.variantAnnotation.get.getAlleleCount == 2)
  //    assert(annotatedAssociations.first.variantAnnotation.get.getAlleleFrequency == 0.333f)
  //    assert(annotatedAssociations.first.variantAnnotation.get.getSomatic == false)
  //    assert(annotatedAssociations.first.variantAnnotation.get.getAttributes.get("ClippingRankSum") == "0.138")
  //  }
  //
  //  sparkTest("Annotations being successfully written to output log file") {
  //
  //    val testOutput = "../test_data_out/annotations_test"
  //    val expectedOutput = "src/test/resources/AnnotationsOutput.txt"
  //
  //    val genoFilePath = ClassLoader.getSystemClassLoader.getResource("small_snpeff.vcf").getFile
  //    val phenoFilePath = ClassLoader.getSystemClassLoader.getResource("2Liner_annot.txt").getFile
  //    val cliCall = s"../bin/gnocchi-submit regressPhenotypes $genoFilePath $phenoFilePath ADDITIVE_LINEAR $testOutput -saveAsText -phenoName pheno1 -overwriteParquet"
  //    val cliArgs = cliCall.split(" ").drop(2)
  //
  //    RegressPhenotypes(cliArgs).run(sc)
  //
  //    val expectedOuputLines = Source.fromFile(expectedOutput).getLines.toSet
  //    val lines = Source.fromFile(testOutput + "/part-00000").getLines.toSet
  //
  //    assert(expectedOuputLines sameElements lines)
  //
  //    FileUtils.deleteDirectory(new File("../test_data_out"))
  //  }

}

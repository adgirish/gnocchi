
package net.fnothaft.gnocchi.association

import breeze.linalg.DenseVector
import net.fnothaft.gnocchi.GnocchiFunSuite
import org.bdgenomics.adam.models.ReferenceRegion
import net.fnothaft.gnocchi.models.Association
import org.bdgenomics.formats.avro.Variant

class AnnotatedVCFHandlingSuite extends GnocchiFunSuite {

  sparkTest("Confirming read in snpEff VCF file and process annotations") {
    val inputPath = testFile("small_snpeff.vcf")
    val vcRdd = sc.loadVcf(inputPath)
    assert(vcRdd.rdd.count === 5)

    for (rv <- vcRdd.rdd) {
      rv.variant.variant
      val variant = rv.variant.variant
      println("TranscriptEffects" + variant.getAnnotation.getTranscriptEffects)
    }
  }
}


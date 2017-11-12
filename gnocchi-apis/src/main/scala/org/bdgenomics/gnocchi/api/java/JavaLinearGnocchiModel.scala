package org.bdgenomics.gnocchi.api.java

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Dataset
import org.bdgenomics.gnocchi.models.variant.{ QualityControlVariantModel, LinearVariantModel }
import org.bdgenomics.gnocchi.models.{ LinearGnocchiModelFactory, GnocchiModel, LinearGnocchiModel }
import org.bdgenomics.gnocchi.primitives.phenotype.Phenotype
import org.bdgenomics.gnocchi.primitives.variants.CalledVariant
import org.bdgenomics.gnocchi.sql.GnocchiSession

import scala.collection.JavaConversions._

object JavaLinearGnocchiModelFactory {

  var gs: GnocchiSession = null

  def generate(gs: GnocchiSession) { this.gs = gs }

  def apply(genotypes: Dataset[CalledVariant],
            phenotypes: scala.collection.immutable.Map[java.lang.String, Phenotype],
            phenotypeNames: java.util.List[java.lang.String], // Option becomes raw object java.util.ArrayList[java.lang.String],
            QCVariantIDs: java.util.List[java.lang.String], // Option becomes raw object
            QCVariantSamplingRate: java.lang.Double,
            allelicAssumption: java.lang.String,
            validationStringency: java.lang.String): LinearGnocchiModel = {

    // Convert python compatible nullable types to scala options
    val phenotypeNamesOption = if (phenotypeNames == null) {
      None
    } else {
      val phenotypeNamesList = asScalaBuffer(phenotypeNames).toList
      Some(phenotypeNamesList)
    }

    val QCVariantIDsOption = if (QCVariantIDs == null) {
      None
    } else {
      val QCVariantIDsList = asScalaBuffer(QCVariantIDs).toSet
      Some(QCVariantIDsList)
    }

    LinearGnocchiModelFactory(genotypes,
      this.gs.sparkSession.sparkContext.broadcast(phenotypes),
      phenotypeNamesOption,
      QCVariantIDsOption,
      QCVariantSamplingRate,
      allelicAssumption,
      validationStringency)
  }
}

class JavaLinearGnocchiModel(val lgm: LinearGnocchiModel) {
  def mergeGnocchiModel(otherModel: JavaLinearGnocchiModel): JavaLinearGnocchiModel = {
    val newModel = lgm.mergeGnocchiModel(otherModel.lgm)
    JavaLinearGnocchiModel(newModel)
  }

  def mergeVariantModels(newVariantModels: Dataset[LinearVariantModel]): Dataset[LinearVariantModel] = {
    lgm.mergeVariantModels(newVariantModels)
  }

  def mergeQCVariants(newQCVariantModels: Dataset[QualityControlVariantModel[LinearVariantModel]]): Dataset[CalledVariant] = {
    lgm.mergeQCVariants(newQCVariantModels)
  }
}

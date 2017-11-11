package org.bdgenomics.gnocchi.api.java

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Dataset
import org.bdgenomics.gnocchi.models.variant.{ QualityControlVariantModel, LogisticVariantModel }
import org.bdgenomics.gnocchi.models.{ LogisticGnocchiModelFactory, GnocchiModel, LogisticGnocchiModel }
import org.bdgenomics.gnocchi.primitives.phenotype.Phenotype
import org.bdgenomics.gnocchi.primitives.variants.CalledVariant
import org.bdgenomics.gnocchi.sql.GnocchiSession

import scala.collection.JavaConversions._

object JavaLogisticGnocchiModelFactory {

  var gs: GnocchiSession = null

  def generate(gs: GnocchiSession) { this.gs = gs }

  def apply(genotypes: Dataset[CalledVariant],
            phenotypes: scala.collection.immutable.Map[java.lang.String, Phenotype],
            phenotypeNames: java.util.List[java.lang.String], // Option becomes raw object java.util.ArrayList[java.lang.String],
            QCVariantIDs: java.util.List[java.lang.String], // Option becomes raw object
            QCVariantSamplingRate: java.lang.Double,
            allelicAssumption: java.lang.String,
            validationStringency: java.lang.String): LogisticGnocchiModel = {

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

    LogisticGnocchiModelFactory(genotypes,
      this.gs.sparkSession.sparkContext.broadcast(phenotypes),
      phenotypeNamesOption,
      QCVariantIDsOption,
      QCVariantSamplingRate,
      allelicAssumption,
      validationStringency)
  }
}

class JavaLogisticGnocchiModel(val lgm: LogisticGnocchiModel) {
  def mergeGnocchiModel(otherModel: JavaLogisticGnocchiModel): GnocchiModel[LogisticVariantModel, LogisticGnocchiModel] = {
    lgm.mergeGnocchiModel(otherModel.lgm)
  }

  def mergeVariantModels(newVariantModels: Dataset[LogisticVariantModel]): Dataset[LogisticVariantModel] = {
    lgm.mergeVariantModels(newVariantModels)
  }

  def mergeQCVariants(newQCVariantModels: Dataset[QualityControlVariantModel[LogisticVariantModel]]): Dataset[CalledVariant] = {
    lgm.mergeQCVariants(newQCVariantModels)
  }
}
package org.bdgenomics.gnocchi.api.java

import org.apache.spark.sql.Dataset
import org.bdgenomics.gnocchi.models.variant.{ QualityControlVariantModel, LinearVariantModel }
import org.bdgenomics.gnocchi.models.{ GnocchiModelMetaData, GnocchiModel, LinearGnocchiModel }
import org.bdgenomics.gnocchi.primitives.phenotype.Phenotype
import org.bdgenomics.gnocchi.primitives.variants.CalledVariant

class JavaLinearGnocchiModel(val lgm: LinearGnocchiModel) {
  def mergeGnocchiModel(otherModel: GnocchiModel[LinearVariantModel, LinearGnocchiModel]): GnocchiModel[LinearVariantModel, LinearGnocchiModel] = {
    lgm.mergeGnocchiModel(otherModel)
  }
}

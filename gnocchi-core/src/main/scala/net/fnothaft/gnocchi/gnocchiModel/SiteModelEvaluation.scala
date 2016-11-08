/**
 * Copyright 2016 Taner Dagdelen
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.fnothaft.gnocchi.gnocchiModel

import net.fnothaft.gnocchi.models._
import net.fnothaft.gnocchi.transformations.PnG2MatchedPairByVariant
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

trait SiteModelEvaluation extends Serializable {

  protected def clipOrKeepState(gs: GenotypeState): Double

  final def apply[T](sc: SparkContext,
                     genotypes: RDD[GenotypeState],
                     phenotypes: RDD[Phenotype[Array[Double]]],
                     models: RDD[GeneralizedLinearSiteModel]): RDD[TestResult] = {

    // match genotypes and phenotypes and organize by variantId
    val phenoGeno = PnG2MatchedPairByVariant(genotypes, phenotypes)
    // join RDD[(variantId, Array[(GenotypeState, Phenotype)])] with RDD[(variantId, GeneralizedLinearSiteModel)]
    // to make RDD[(variantId, (Array[(GenotypeState, Phenotype)], GeneralizedLinearSiteModel))]
    phenoGeno.join(models.keyBy(_.variantId))
      // build or update model for each site
      .map(site => {
        // group by site the phenotype and genotypes. observations is an array of (GenotypeState, Phenotype) tuples.
        val (variantId, (obs, model)) = site
        // call predict of the model for each site on the data for each site.
        evaluateModelAtSite(sc, variantId, (obs, model))
      })
  }

  protected def evaluateModelAtSite(sc: SparkContext,
                                    variantId: String,
                                    dataAndModel: (Array[(GenotypeState, Phenotype[Array[Double]])], GeneralizedLinearSiteModel)): (String, Array[TestResult])

}
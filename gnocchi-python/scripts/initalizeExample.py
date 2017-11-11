from bdgenomics.gnocchi.gnocchiSession import GnocchiSession
from bdgenomics.gnocchi.linearGnocchiModel import LinearGnocchiModel
from bdgenomics.gnocchi.logisticGnocchiModel import LogisticGnocchiModel
from bdgenomics.gnocchi.regressPhenotypes import RegressPhenotypes

genotypesPath1 = "../examples/testData/time_genos_1.vcf"
phenotypesPath1 = "../examples/testData/tab_time_phenos_1.txt"

gs = GnocchiSession(spark)
genos = gs.loadGenotypes(genotypesPath1)
phenos = gs.loadPhenotypes(phenotypesPath1, "IID", "pheno_1", "\t")

lgm = LinearGnocchiModel(spark, genos.get(), phenos.get(), ["AD"], ["GI"])
lgm2 = LinearGnocchiModel(spark, genos.get(), phenos.get(), ["AD"], ["GI"])


lgm.mergeGnocchiModel(lgm2)

logm = LogisticGnocchiModel(spark, genos.get(), phenos.get(), ["AD"], ["GI"])
logm2 = LogisticGnocchiModel(spark, genos.get(), phenos.get(), ["AD"], ["GI"])


logm.mergeGnocchiModel(lgm2)

from bdgenomics.gnocchi.regressPhenotypes import RegressPhenotypes
rp = RegressPhenotypes(spark)
rp.apply("../examples/testData/1snp10samples.vcf ../examples/testData/10samples1Phenotype.txt ADDITIVE_LINEAR ../examples/testData/DELETEME -saveAsText -sampleIDName SampleID -phenoName pheno1 -overwriteParquet")
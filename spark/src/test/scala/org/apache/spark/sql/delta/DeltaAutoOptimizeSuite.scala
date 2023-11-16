/*
 * Copyright (2021) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

 package org.apache.spark.sql.delta
 
 import org.apache.hadoop.fs.Path
 import org.apache.spark.sql.test.SharedSparkSession
 import org.apache.spark.sql.QueryTest
 import org.apache.spark.sql.delta.actions._
 import org.apache.spark.sql.delta.sources.DeltaSQLConf
 import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
 import org.apache.spark.sql.functions._

class DeltaAutoOptimizeSuite extends QueryTest 
  with SharedSparkSession
  with DeltaSQLCommandTest
  with DeletionVectorsTestUtils {

  // def writeDataToCheckAutoOptimize(
  //   numFiles: Int,
  //   dataPath: String,
  //   partitioned: Boolean = false,
  //   mode: String = "overwrite"): Unit = {

  //   val df = spark
  //   .range(50000)
  //   .withColumn("colA", rand() * 10000000 cast "long")
  //   .withColumn("colB", rand() * 1000000000 cast "int")
  //   .withColumn("colC", rand() * 2 cast "int")
  //   .drop("id")
  //   .repartition(numFiles)

  //   if (partitioned) {
  //     df.write
  //       .partitionBy("colC")
  //       .mode(mode)
  //       .format("delta")
  //       .save(dataPath)
  //   } else {
  //     df.write
  //       .mode(mode)
  //       .format("delta")
  //       .save(dataPath)
  //   }
  // }

  // def checkTableVersionAndNumFiles(
  //   path: String,
  //   expectedVersion: Long,
  //   expectedNumFiles: Long): Unit = {
  //     val dt = DeltaLog.forTable(spark, path)
  //     assert(dt.unsafeVolatileSnapshot.allFiles.count() == expectedNumFiles)
  //     assert(dt.unsafeVolatileSnapshot.version == expectedVersion)
  // }

  // test("test enable autoOptimize") {
  //   val tableName = "autoOptimizeTable"
  //   val tableName2 = s"${tableName}2"
  //   withTable(tableName, tableName2) {
  //     withTempDir { dir =>
  //       val rootPath = dir.getCanonicalPath
  //       val path = new Path(rootPath, "table1").toString
  //       var expectedTableVersion = -1
  //       spark.conf.unset(DeltaSQLConf.DELTA_AUTO_OPTIMIZE_ENABLED.key)
  //       writeDataToCheckAutoOptimize(100, path)
  //       // No autoOptimize triggered: version should be 0
  //       expectedTableVersion += 1
  //       checkTableVersionAndNumFiles(path, expectedTableVersion, 100)

  //       // Create table
  //       spark.sql(s"CREATE TABLE $tableName USING DELTA LOCATION '$path'")
  //       spark.sql(s"ALTER TABLE $tableName SET TBLPROPERTIES (delta.autoOptimize = true)")
  //       expectedTableVersion += 1 // Version changed due to ALTER TABLE
  //       checkTableVersionAndNumFiles(path, expectedTableVersion, 100)

  //       writeDataToCheckAutoOptimize(100, path)
  //       expectedTableVersion += 2 // autoOptimize should be triggered
  //       checkTableVersionAndNumFiles(path, expectedTableVersion, 1)

  //       withSQLConf(DeltaSQLConf.DELTA_AUTO_OPTIMIZE_ENABLED.key -> "false") {
  //         // Session config takes priority over table properties
  //         writeDataToCheckAutoOptimize(100, path)
  //         expectedTableVersion += 1
  //         checkTableVersionAndNumFiles(path, expectedTableVersion, 100)
  //       }

  //       spark.sql(s"ALTER TABLE $tableName SET TBLPROPERTIES (delta.autoOptimize = false)")
  //       expectedTableVersion += 1 // Version changed due to ALTER TABLE

  //       withSQLConf(DeltaSQLConf.DELTA_AUTO_OPTIMIZE_ENABLED.key -> "true") {
  //         // Session config takes priority over table properties
  //         writeDataToCheckAutoOptimize(100, path)
  //         expectedTableVersion += 2
  //         checkTableVersionAndNumFiles(path, expectedTableVersion, 1)
  //       }

  //       spark.conf.unset(DeltaSQLConf.DELTA_AUTO_OPTIMIZE_ENABLED.key)

  //       withSQLConf("spark.databricks.delta.properties.defaults.autoOptimize" -> "true") {
  //         val path3 = new Path(rootPath, "table3").toString
  //         writeDataToCheckAutoOptimize(100, path3)
  //         // autoOptimize should be triggered for path3
  //         checkTableVersionAndNumFiles(path3, 1, 1)
  //       }
  //     }
  //   }
  // }

  // test("test autoOptimize configs") {
  //   val tableName = "autoOptimizeTestTable"
  //   withTable(tableName) {
  //     withTempDir { dir =>
  //       val rootPath = dir.getCanonicalPath
  //       val path = new Path(rootPath, "table1").toString
  //       var expectedTableVersion = -1;
  //       withSQLConf(DeltaSQLConf.DELTA_AUTO_OPTIMIZE_ENABLED.key -> "true") {
  //         writeDataToCheckAutoOptimize(100, path, partitioned = true)
  //         expectedTableVersion += 2 // autoOptimize should be triggered
  //         checkTableVersionAndNumFiles(path, expectedTableVersion, 2)

  //         withSQLConf(DeltaSQLConf.DELTA_AUTO_OPTIMIZE_MIN_NUM_FILES.key -> "200") {
  //           writeDataToCheckAutoOptimize(100, path, partitioned = true)
  //           expectedTableVersion += 1 // autoOptimize should not be triggered
  //           checkTableVersionAndNumFiles(path, expectedTableVersion, 200)
  //         }

  //         withSQLConf(DeltaSQLConf.DELTA_AUTO_OPTIMIZE_MAX_FILE_SIZE.key -> "1") {
  //           writeDataToCheckAutoOptimize(100, path, partitioned = true)
  //           expectedTableVersion += 1 // autoOptimize should not be triggered
  //           checkTableVersionAndNumFiles(path, expectedTableVersion, 200)
  //         }

  //         withSQLConf(DeltaSQLConf.DELTA_OPTIMIZE_MAX_FILE_SIZE.key -> "101024",
  //           DeltaSQLConf.DELTA_AUTO_OPTIMIZE_MIN_NUM_FILES.key -> "2") {
  //           val dt = io.delta.tables.DeltaTable.forPath(path)
  //           dt.optimize().executeCompaction()
  //           expectedTableVersion += 1 // autoOptimize should not be triggered
  //           checkTableVersionAndNumFiles(path, expectedTableVersion, 8)
  //         }

  //         withSQLConf(DeltaSQLConf.DELTA_AUTO_OPTIMIZE_MIN_FILE_SIZE.key ->  "100") {
  //           writeDataToCheckAutoOptimize(100, path, partitioned = true)
  //           expectedTableVersion += 1 // autoOptimize should not be triggered
  //           checkTableVersionAndNumFiles(path, expectedTableVersion, 200)
  //         }

  //         withSQLConf(DeltaSQLConf.DELTA_AUTO_OPTIMIZE_MIN_NUM_FILES.key -> "100") {
  //           writeDataToCheckAutoOptimize(100, path, partitioned = true)
  //           expectedTableVersion += 2 // autoOptimize should be triggered
  //           checkTableVersionAndNumFiles(path, expectedTableVersion, 2)
  //         } 
  //       } 
  //     } 
  //   }
  // }

  // test("test max optimzie data size config") {
  //   withTempDir { dir =>
  //     val rootPath = dir.getCanonicalPath
  //     val path = new Path(rootPath, "table1").toString
  //     var expectedTableVersion = -1
  //     writeDataToCheckAutoOptimize(100, path, partitioned = true)
  //     expectedTableVersion += 1
  //     checkTableVersionAndNumFiles(path, expectedTableVersion, 200)
  //     val dt = io.delta.tables.DeltaTable.forPath(path)
  //     val dl = DeltaLog.forTable(spark, path)
  //     val sizeLimit = dl.unsafeVolatileSnapshot
  //       .allFiles
  //       .filter(col("path").contains("colC=1"))
  //       .agg(sum(col("size")))
  //       .head
  //       .getLong(0) * 2

  //     withSQLConf(DeltaSQLConf.DELTA_AUTO_OPTIMIZE_ENABLED.key -> "true",
  //       DeltaSQLConf.DELTA_AUTO_OPTIMIZE_MAX_COMPACT_BYTES.key -> sizeLimit.toString) {
  //       dt.toDF
  //         .filter("colC == 1")
  //         .repartition(50)
  //         .write
  //         .format("delta")
  //         .mode("append")
  //         .save(path)
  //       val dl = DeltaLog.forTable(spark, path)
  //       // version 0: write, 1: append, 2: autoOptimize
  //       assert(dl.unsafeVolatileSnapshot.version == 2)

  //       {
  //         val beforeAutoOptimize = dl.getSnapshotAt(dl.unsafeVolatileSnapshot.version - 1)
  //           .allFiles
  //           .filter(col("path").contains("colC=1"))
  //           .count
  //         assert(beforeAutoOptimize == 150)

  //         val afterAutoOptimize = dl.unsafeVolatileSnapshot
  //           .allFiles
  //           .filter(col("path").contains("colC=1"))
  //           .count
  //         assert(afterAutoOptimize == 1)
  //       }

  //       {
  //         val beforeAutoOptimize = dl.getSnapshotAt(dl.unsafeVolatileSnapshot.version - 1)
  //           .allFiles
  //           .filter(col("path").contains("colC=0"))
  //           .count
  //         assert(beforeAutoOptimize == 100)

  //         val afterAutoOptimize = dl.unsafeVolatileSnapshot
  //           .allFiles
  //           .filter(col("path").contains("colC=0"))
  //           .count
  //         assert(afterAutoOptimize == 100)
  //       }
  //     }     
  //   }
  // }

  // test("test autoOptimize.target config") {
  //   withTempDir { dir =>
  //     val rootPath = dir.getCanonicalPath
  //     val path1 = new Path(rootPath, "table1").toString
  //     val path2 = new Path(rootPath, "table2").toString
  //     val path3 = new Path(rootPath, "table3").toString
  //     val path4 = new Path(rootPath, "table4").toString
  //     val path5 = new Path(rootPath, "table5").toString

  //     def testautoOptimizeTarget(
  //       path: String,
  //       target: String,
  //       expectedColC1Cnt: Long,
  //       expectedColC0Cnt: Long): Unit = {
  //       writeDataToCheckAutoOptimize(100, path, partitioned = true)
  //       val dt = io.delta.tables.DeltaTable.forPath(path)

  //       withSQLConf(
  //         DeltaSQLConf.DELTA_AUTO_OPTIMIZE_ENABLED.key -> "true",
  //         DeltaSQLConf.DELTA_AUTO_OPTIMIZE_TARGET.key -> target) {
  //         dt.toDF
  //           .filter("colC == 1")
  //           .repartition(50)
  //           .write
  //           .format("delta")
  //           .mode("append")
  //           .save(path)

  //         val dl = DeltaLog.forTable(spark, path)
  //         // version 0: write, 1: append, 2: autoOptimize
  //         assert(dl.unsafeVolatileSnapshot.version == 2, target)

  //         {
  //           val beforeAutoOptimize = dl.getSnapshotAt(dl.unsafeVolatileSnapshot.version - 1)
  //             .allFiles
  //             .filter(col("path").contains("colC=1"))
  //             .count
  //           assert(beforeAutoOptimize == 150)

  //           val afterAutoOptimize = dl.unsafeVolatileSnapshot
  //             .allFiles
  //             .filter(col("path").contains("colC=1"))
  //             .count
  //           assert(afterAutoOptimize == expectedColC1Cnt)
  //         }

  //         {
  //           val beforeAutoOptimize = dl.getSnapshotAt(dl.unsafeVolatileSnapshot.version - 1)
  //             .allFiles
  //             .filter(col("path").contains("colC=0"))
  //             .count
  //           assert(beforeAutoOptimize == 100)

  //           val afterAutoOptimize = dl.unsafeVolatileSnapshot
  //             .allFiles
  //             .filter(col("path").contains("colC=0"))
  //             .count
  //           assert(afterAutoOptimize == expectedColC0Cnt)
  //         }
  //       }
  //     }

  //     // Existing files are not optimized; newly added 50 files should be optimized.
  //     // 100 for colC=0 and 101 for colC=1
  //     testautoOptimizeTarget(path1, "commit", 101, 100)
  //     // Modified partition should be optimzied.
  //     // 100 for colC=0 and 1 for colC=1
  //     testautoOptimizeTarget(path2, "partition", 1, 100)
  //     // table optimzie should optimize all partitions. 
  //     testautoOptimizeTarget(path3, "table", 1, 1)

  //     withSQLConf(
  //       "spark.databricks.delta.autoOptimize.enabled" -> "true", 
  //       "spark.databricks.delta.autoOptimize.target" -> "partition") {
  //       writeDataToCheckAutoOptimize(100, path4)
  //       // non-partitioned data should be optimized with the "partition" option
  //       checkTableVersionAndNumFiles(path4, 1, 1)
  //     }

  //     withSQLConf(
  //       DeltaSQLConf.DELTA_AUTO_OPTIMIZE_ENABLED.key -> "true", 
  //       DeltaSQLConf.DELTA_AUTO_OPTIMIZE_TARGET.key -> "partition") {
  //       writeDataToCheckAutoOptimize(100, path5)
  //       // non-partitioned data should be optimized with the "partition" option
  //       checkTableVersionAndNumFiles(path5, 1, 1)
  //     }

  //     val e = intercept[IllegalArgumentException](
  //       withSQLConf(DeltaSQLConf.DELTA_AUTO_OPTIMIZE_TARGET.key -> "tabel") {
  //         writeDataToCheckAutoOptimize(10, path1, partitioned = true)
  //     })
  //     assert(e.getMessage.contains("should be one of table, commit, partition, but was tabel"))
  //   }
  // }

  // test("test autoOptimize with DeletionVectors") {
  //   withTempDir { tempDir =>
  //     val path = tempDir.getAbsolutePath
  //     withSQLConf(
  //       DeltaConfigs.ENABLE_DELETION_VECTORS_CREATION.defaultTablePropertyKey -> "true") {
  //       // Create 47 files with 1000 records each. 
  //       spark.range(start = 0, end = 1000, step = 1, numPartitions = 47)
  //         .toDF("id")
  //         .withColumn(colName = "extra", lit("A random string to add bytes used by the table."))
  //         .write
  //         .format("delta")
  //         .save(path) // v0
        
  //       val dl = DeltaLog.forTable(spark, path)
  //       val filesV0 = dl.unsafeVolatileSnapshot.allFiles.collect()
  //       assert(filesV0.size == 47)

  //       // Default 'optimize.maxDeleteRowsRatio' is 0.05
  //       // Delete slightly more than the threshold in two files, and less in one of the files
  //       deleteRows(dl, filesV0(0), approxPhyRows = 1000, ratioOfRowsToDelete = 0.06) // v1
  //       deleteRows(dl, filesV0(4), approxPhyRows = 1000, ratioOfRowsToDelete = 0.03) // v2
  //       deleteRows(dl, filesV0(8), approxPhyRows = 1000, ratioOfRowsToDelete = 0.07) // v3

  //       // Save the data before optimize for comparing it later with optimzie
  //       val data = spark.read.format("delta").load(path)

  //       withSQLConf(DeltaSQLConf.DELTA_AUTO_OPTIMIZE_ENABLED.key -> "true",
  //         DeltaSQLConf.DELTA_AUTO_OPTIMIZE_TARGET.key -> "table") {
  //         data.write.format("delta").mode("append").save(path) // v4 and v5
  //       }
  //       val appendChanges = dl.getChanges(startVersion = 4).next()._2
  //       val autoOptimizeChanges = dl.getChanges(startVersion = 5).next()._2

  //       // We expect the initial files and the ones from the last append to be compacted
  //       val expectedRemoveFiles = (filesV0 ++ addedFiles(appendChanges)).map(_.path).toSet

  //       assert(removedFiles(autoOptimizeChanges).map(_.path).toSet === expectedRemoveFiles)
  //       assert(addedFiles(autoOptimizeChanges).size == 1) //  Expect one new file to be added

  //       //Verify the final data after optimization has not changed
  //       checkAnswer(spark.read.format("delta").load(path), data)
  //     }
  //   }
  // }


  // private def removedFiles(actions: Seq[Action]): Seq[RemoveFile] = {
  //   actions.filter(_.isInstanceOf[RemoveFile]).map(_.asInstanceOf[RemoveFile])
  // }
  
  // private def addedFiles(actions: Seq[Action]): Seq[AddFile] = {
  //   actions.filter(_.isInstanceOf[AddFile]).map(_.asInstanceOf[AddFile])
  // }
}
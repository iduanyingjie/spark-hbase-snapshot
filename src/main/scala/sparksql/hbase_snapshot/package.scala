package sparksql

import org.apache.hadoop.hbase.HConstants
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.DataType
import org.slf4j.LoggerFactory

package object hbase_snapshot {

  private[hbase_snapshot] val LOG = LoggerFactory.getLogger("hbase_snapshot")

  abstract class SchemaField extends Serializable

  private[hbase_snapshot] case class RegisteredSchemaField(fieldName: String, fieldType: String) extends SchemaField with Serializable {
    override def toString: String = s"[fieldName: $fieldName, fieldType: $fieldType]"
  }

  private[hbase_snapshot] case class HBaseSchemaField(fieldName: String, fieldType: String) extends SchemaField with Serializable {
    override def toString: String = s"[fieldName: $fieldName, fieldType: $fieldType]"
  }

  private[hbase_snapshot] case class SchemaType(dataType: DataType, nullable: Boolean)

  protected[hbase_snapshot] val SPARK_SQL_TABLE_SCHEMA  = "sparksql.table.schema"
  protected[hbase_snapshot] val HBASE_ZOOKEEPER_QUORUM  = HConstants.ZOOKEEPER_QUORUM
  protected[hbase_snapshot] val HBASE_TABLE_NAME        = "hbase.table.name"
  protected[hbase_snapshot] val HBASE_TABLE_SCHEMA      = "hbase.table.schema"
  protected[hbase_snapshot] val HBASE_ROOTDIR           = "hbase.rootdir"
  protected[hbase_snapshot] val HBASE_SNAPSHOT_VERSIONS = "hbase.snapshot.versions"
  protected[hbase_snapshot] val HBASE_SNAPSHOT_PATH     = "hbase.snapshot.path"
  protected[hbase_snapshot] val HBASE_SNAPSHOT_NAME     = "hbase.snapshot.name"

  protected[hbase_snapshot] val defaultSnapshotVersions = "3"
  protected[hbase_snapshot] val defaultHBaseRootDir     = "/hbase"

  /**
    * Adds a method, `hbaseTable`, to SQLContext that allows reading data stored in hbase table.
    */
  implicit class HBaseSnapshotContext(sqlContext: SQLContext) {
    def hbaseTable(sparksqlTableSchema: String,
                   zookeeperQuorum: String,
                   hbaseTableName: String,
                   hbaseTableSchema: String,
                   hbaseRootDir: String = defaultHBaseRootDir,
                   hbaseSnapshotVersions: String = defaultSnapshotVersions,
                   hbaseSnapshotPath: String,
                   hbaseSnapshotName: String) = {
      sqlContext.baseRelationToDataFrame(HBaseSnapshotRelation(Map(
        SPARK_SQL_TABLE_SCHEMA -> sparksqlTableSchema,
        HBASE_ZOOKEEPER_QUORUM -> zookeeperQuorum,
        HBASE_TABLE_NAME -> hbaseTableName,
        HBASE_TABLE_SCHEMA -> hbaseTableSchema,
        HBASE_ROOTDIR -> hbaseRootDir,
        HBASE_SNAPSHOT_VERSIONS -> hbaseSnapshotVersions,
        HBASE_SNAPSHOT_PATH -> hbaseSnapshotPath,
        HBASE_SNAPSHOT_NAME -> hbaseSnapshotName
      ))(sqlContext))
    }
  }

}

package sparksql.hbase_snapshot

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableSnapshotInputFormat}
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}

case class HBaseSnapshotRelation(@transient val hbaseProps: Map[String, String])
                                (@transient val sqlContext: SQLContext) extends BaseRelation with Serializable with TableScan {

  val registerTableSchema   = hbaseProps.getOrElse(SPARK_SQL_TABLE_SCHEMA, sys.error("not valid register table schema"))
  val hbaseZookeeperQuorum  = hbaseProps.getOrElse(HBASE_ZOOKEEPER_QUORUM, sys.error("not valid zookeeper quorum"))
  val hbaseTableName        = hbaseProps.getOrElse(HBASE_TABLE_NAME, sys.error("not valid hbase table name"))
  val hbaseTableSchema      = hbaseProps.getOrElse(HBASE_TABLE_SCHEMA, sys.error("not valid hbase table schema"))
  val hbaseRootDir          = hbaseProps.getOrElse(HBASE_ROOTDIR, defaultHBaseRootDir)
  val hbaseSnapshotVersions = hbaseProps.getOrElse(HBASE_SNAPSHOT_VERSIONS, defaultSnapshotVersions).toInt
  val hbaseSnapshotPath     = hbaseProps.getOrElse(HBASE_SNAPSHOT_PATH, sys.error("not valid hbase snapshot path"))
  val hbaseSnapshotName     = hbaseProps.getOrElse(HBASE_SNAPSHOT_NAME, sys.error("not valid hbase snapshot name"))

  // '(:key , f1:col1 )'
  val tempHBaseFields =
    hbaseTableSchema.trim.drop(1).dropRight(1).split(",")
      .map { fildString => HBaseSchemaField(fildString.trim, "") }
      .toList

  // '(rowkey string, value string, column_a string)'
  val registerTableFields =
    registerTableSchema.trim.drop(1).dropRight(1).split(",")
      .map { fildString =>
        val splitedField = fildString.trim.split("\\s+", -1)
        RegisteredSchemaField(splitedField(0), splitedField(1))
      }.toList

  val tempFieldRelation =
    if (tempHBaseFields.length != registerTableFields.length) sys.error("columns size not match in definition!")
    else tempHBaseFields zip registerTableFields

  val hbaseTableFields = tempFieldRelation.map { case (k, v) => k.copy(fieldType = v.fieldType) }

  val fieldsRelations =
    if (hbaseTableFields.length != registerTableFields.length) sys.error("columns size not match in definition!")
    else (hbaseTableFields zip registerTableFields).toMap

  LOG.info(
    s"""
       |------------------------------------------------------------------------------------
       |HBase Table Fields: ${hbaseTableFields.mkString("\n  ", "\n  ", "\n")}
       |Spark Table Fields: ${registerTableFields.mkString("\n  ", "\n  ", "\n")}
       |Fields Relations:  ${fieldsRelations.mkString("\n  ", "\n  ", "\n")}
       |------------------------------------------------------------------------------------
  """.stripMargin)

  override def schema: StructType = StructType(
    hbaseTableFields.map { field =>
      val name = fieldsRelations.getOrElse(field, sys.error("table schema is not match the definition.")).fieldName
      val relatedType = field.fieldType match {
        case "string" => StringType
        case "int" => IntegerType
        case "long" => LongType
        case "float" => FloatType
        case "double" => DoubleType
      }
      StructField(name, relatedType)
    }
  )

  override def buildScan(): RDD[Row] = {
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set(HBASE_ROOTDIR, hbaseRootDir)
    hbaseConf.set(HBASE_ZOOKEEPER_QUORUM, hbaseZookeeperQuorum)
    hbaseConf.set(TableInputFormat.SCAN, convertScanToString(new Scan().setMaxVersions(hbaseSnapshotVersions)))

    val job = Job.getInstance(hbaseConf)
    TableSnapshotInputFormat.setInput(job, hbaseSnapshotName, new Path(hbaseSnapshotPath))

    val hbaseRdd = sqlContext.sparkContext.newAPIHadoopRDD(
      job.getConfiguration,
      classOf[TableSnapshotInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result]
    )

    hbaseRdd.map { tuple =>
      Row.fromSeq(hbaseTableFields.map(field => Resolver.resolve(field, tuple._2)))
    }
  }

  def convertScanToString(scan: Scan) = {
    val proto = ProtobufUtil.toScan(scan)
    Base64.encodeBytes(proto.toByteArray())
  }

}

object Resolver extends Serializable {

  def resolve(hbaseField: HBaseSchemaField, result: Result): Any = {
    val cfColArray = hbaseField.fieldName.split(":", -1)
    val cfName = cfColArray(0)
    val colName = cfColArray(1)
    //resolve row key otherwise resolve column
    if (cfName == "" && colName == "key") {
      resolveRowKey(result, hbaseField.fieldType)
    }
    else {
      resolveColumn(result, cfName, colName, hbaseField.fieldType)
    }
  }

  def resolveRowKey(result: Result, resultType: String): Any = bytesToAny(resultType, result.getRow)

  def resolveColumn(result: Result, columnFamily: String,
                    columnName: String, resultType: String): Any = {
    val cfBytes = columnFamily.getBytes
    val cnBytes = columnName.getBytes
    if (result.containsColumn(cfBytes, cnBytes)) {
      bytesToAny(resultType, result.getValue(cfBytes, cnBytes))
    }
    else null
  }

  def bytesToAny(resultType: String, bytes: Array[Byte]) = resultType match {
    case "string" => Bytes.toString(bytes)
    case "int" => Bytes.toInt(bytes)
    case "long" => Bytes.toLong(bytes)
    case "float" => Bytes.toFloat(bytes)
    case "double" => Bytes.toDouble(bytes)
  }


}
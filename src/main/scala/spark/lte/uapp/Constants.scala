package spark.lte.uapp

object Constants {
  val downLoadColName = "transaction_downlink_bytes"
  val upLoadColName = "transaction_uplink_bytes"
  val totalBytesColName = "transaction_total_bytes"
  val tableName = "edr_tab_main"
  val dbName = "sid_db"
  val httpUrlColName = "http_url"
  val domainNameColName = "domain_name"
  val httpContentTypeColName = "http_content_type"
  val subscriberColName = "radius_user_name"
  val httpContentTypeDataPath = "/data/siddharth/http_content_type_data.csv"
  val replyCodeColName = "http_reply_code"
  val urlHitsColName = "url_hits_count"
  val ggsnXMLPath = "/data/siddharth/ggsn.xml"
  val ggsnIPColName = "ggsn_ip"
  val ggsnNameColName = "ggsn_name"
}

CREATE EXTERNAL TABLE IF NOT EXISTS `music_brains`.`music_artists_all` (
  `mbid` string,
  `artist_mb` string,
  `artist_lastfm` string,
  `country_mb` string,
  `country_lastfm` string,
  `tags_mb` string,
  `tags_lastfm` string,
  `listeners_lastfm` string,
  `scrobbles_lastfm` string,
  `ambiguous_artist` string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  'separatorChar' = '\t',
  'quoteChar' = '\'',
  'escapeChar' = '\\'
)
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://music-951f64e0/csv/'
TBLPROPERTIES ('classification' = 'csv');

SELECT * FROM "music_brains"."music_artists_all" limit 10;

SELECT count(*) FROM "music_brains"."music_artists_all";

SELECT count(*) FROM "music_brains"."music_artists_all" where ambiguous_artist = 'TRUE';
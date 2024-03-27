
Create database music;

CREATE EXTERNAL TABLE IF NOT EXISTS `music`.`artists` (
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
STORED AS PARQUET
LOCATION 's3://music-d6cb4270/parquet';

--## country/artists count 

select * from artists where mb_id in (
select mb_ib, artist_lastfm, count(*) from artists 
where ambiguous_artist = 'FALSE'
group by mb_ib, artist_lastfm
having count(*) > 10) grouped;

--## cleaned 

select * from artists where 
ambiguous_artist is not null 
and country_lastfm is not null
and country_mb is not null
and artist_mb is not null
and artist_lastfm is not null
and tags_mb is not null
and tags_lastfm is not null
and mbid != 'mbid';

--#### most listeners

Select 
artist_lastfm artist_name,
country_mb country_origin,
split(country_lastfm,';') country_list,
cast(listeners_lastfm as int) listeners_cnt,
cast(scrobbles_lastfm as int) scrobbles_cnt,
split(tags_mb,';') tags 
from artists 
where mbid != 'mbid'
and country_lastfm != ''
and artist_mb != ''
order by listeners_cnt desc

hive -e 'Select 
artist_lastfm artist_name,
country_mb country_origin,
split(country_lastfm,';') country_list,
cast(listeners_lastfm as int) listeners_cnt,
cast(scrobbles_lastfm as int) scrobbles_cnt,
split(tags_mb,';') tags 
from artists 
where mbid != 'mbid'
and country_lastfm != ''
and artist_mb != ''
order by listeners_cnt desc' | sed 's/[\t]/,/g'  > /final.csv

hive -e 'Select artist_lastfm artist_name,country_mb country_origin,split(country_lastfm,";") as country_list, cast(listeners_lastfm as int) listeners_cnt , cast(scrobbles_lastfm as int) scrobbles_cnt , split(tags_mb,";") tags from music.artists where mbid != "mbid" and country_lastfm != "" and artist_mb != "" order by listeners_cnt desc' > /final.csv



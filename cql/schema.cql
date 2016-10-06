CREATE KEYSPACE IF NOT EXISTS analytics WITH replication = {'class':'SimpleStrategy', 'replication_factor' :1};
/*
CREATE TABLE IF NOT EXISTS analytics.post_points(
  day varchar,
  video_id varchar,
  id varchar,
  views int,
  likes int,
  dislikes int,
  estimated_minutes_watched double,
  average_view_duration double,
  average_view_percentage double,
  favorites_added int,
  favorites_removed int,
  annotation_close_rate double,
  annotation_click_through_rate double,
  subscribers_gained int,
  subscribers_lost int,
  shares int,
  comments int,
  uniques int,
  uniques_7day int,
  uniques_30day int,
  PRIMARY KEY(day,video_id)
) WITH COMPACT STORAGE;
*/
CREATE TABLE IF NOT EXISTS analytics.post_points(
  day varchar,
  video_id varchar,
  views int,
  PRIMARY KEY(day,video_id)
) WITH COMPACT STORAGE;

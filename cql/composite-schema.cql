CREATE KEYSPACE IF NOT EXISTS analytics WITH replication = {'class':'SimpleStrategy', 'replication_factor' :1};
CREATE TABLE IF NOT EXISTS analytics.post_points(
  day varchar,
  video_id varchar,
  networks list<text>,
  views int,
  PRIMARY KEY(day,video_id)
);
CREATE INDEX groups_index ON post_points(groups);

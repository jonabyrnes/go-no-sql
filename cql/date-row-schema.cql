DROP KEYSPACE IF EXISTS analytics;
CREATE KEYSPACE analytics WITH replication = {'class':'SimpleStrategy', 'replication_factor' :1};
CREATE TABLE analytics.post_metrics (
  day text,
  post_id text,
  metrics text,
  PRIMARY KEY(post_id, day)
) WITH COMPACT STORAGE;

// SELECT * FROM analytics.post_metrics where day ='2016-04-01' and post_id in ('49','34','19');
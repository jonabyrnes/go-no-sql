CREATE KEYSPACE analytics WITH replication = {'class':'SimpleStrategy', 'replication_factor' :1};
CREATE TABLE IF NOT EXISTS analytics.group_types(id int PRIMARY KEY, name text, groups list<int>);
CREATE TABLE IF NOT EXISTS analytics.post_points(
  id text PRIMARY KEY,
  groups list<int>,
  day timestamp,
  views int,
  likes int,
  dislikes int,
  estimatedMinutesWatched double,
  averageViewDuration double,
  averageViewPercentage double,
  favoritesAdded int,
  favoritesRemoved int,
  annotationCloseRate double,
  annotationClickThroughRate double,
  subscribersGained int,
  subscribersLost int,
  shares int,
  comments int,
  video_id int,
  updated timestamp,
  uniques int,
  uniques_7day int,
  uniques_30day int
);

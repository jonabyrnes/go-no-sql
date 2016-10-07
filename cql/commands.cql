
INSERT INTO post_points (day, video_id, networks, views) VALUES ('2016-01-01', '1234', ['10','1','5'], 20);
INSERT INTO post_points (day, video_id, networks, views) VALUES ('2016-01-01', '5678', ['3','12','5'], 30);
INSERT INTO post_points (day, video_id, networks, views) VALUES ('2016-01-02', '1234', ['10','1','5'], 5);
INSERT INTO post_points (day, video_id, networks, views) VALUES ('2016-01-02', '5678', ['3','12','5'], 7);

SELECT * FROM post_points where networks contains 10;




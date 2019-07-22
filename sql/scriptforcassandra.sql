CREATE KEYSPACE vkspace WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 };
CREATE TABLE vkspace.twitterdata (id VARCHAR PRIMARY KEY, created_at VARCHAR , favorite_count VARCHAR );
CREATE TABLE vkspace.toptentag (count int PRIMARY KEY, tags VARCHAR);
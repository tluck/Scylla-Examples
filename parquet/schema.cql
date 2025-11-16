CREATE KEYSPACE IF NOT EXISTS my_keyspace
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE IF NOT EXISTS my_keyspace.users (
    user_id bigint,                -- unique user key (partition key)
    name text,                     -- user name
    email text,                    -- email address
    age int,                       -- age in years
    signup_date date,              -- YYYY-MM-DD
    is_active boolean,             -- True/False flag
    tags list<text>,               -- flexible attribute
    PRIMARY KEY (user_id)
);


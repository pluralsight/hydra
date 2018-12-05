CREATE SCHEMA ingest;

CREATE TABLE ingest."group" (
  id SERIAL PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  created_date TIMESTAMP NOT NULL,
  modified_date TIMESTAMP NOT NULL
);

CREATE TABLE ingest.resource (
  id SERIAL PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  resource_type VARCHAR(255) NOT NULL,
  group_id INT,
  FOREIGN KEY (group_id) references ingest."group"(id)
);

CREATE TABLE ingest.token (
  id SERIAL PRIMARY KEY,
  created_date TIMESTAMP NOT NULL,
  modified_date TIMESTAMP,
  token VARCHAR(255) NOT NULL,
  group_id INT NOT NULL,
  FOREIGN KEY (group_id) references ingest."group"(id)
 );
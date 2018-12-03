CREATE TABLE "group" (
  id INT PRIMARY KEY AUTO_INCREMENT,
  name VARCHAR(255) NOT NULL,
  created_date TIMESTAMP NOT NULL,
  modified_date TIMESTAMP NOT NULL
);

CREATE TABLE resource (
  id INT PRIMARY KEY AUTO_INCREMENT,
  name VARCHAR(255) NOT NULL,
  resource_type VARCHAR(255) NOT NULL,
  group_id INT,
  FOREIGN KEY (group_id) references "group"(id)
);

CREATE TABLE token (
   id INT PRIMARY KEY AUTO_INCREMENT,
   created_date TIMESTAMP NOT NULL,
   modified_date TIMESTAMP,
   token VARCHAR(255) NOT NULL,
   group_id INT NOT NULL,
   FOREIGN KEY (group_id) references "group"(id)
 );

INSERT INTO "group" (name, created_date, modified_date) VALUES
    ('test-group', '2018-11-29 00:00:00', '2018-11-29 00:00:00');

INSERT INTO resource (name, resource_type, group_id) VALUES
    ('resourceA', 'topic', 1),
    ('resourceB', 'topic', 1);

INSERT INTO token (created_date, modified_date, token, group_id) VALUES
    ('2018-11-29 00:00:00', '2018-11-29 00:00:00', 'test-token', 1),
    ('2018-11-29 00:00:00', '2018-11-29 00:00:00', 'nope-token', 1),
    ('2018-11-29 00:00:00', '2018-11-29 00:00:00', 'to-delete-token', 1);
CREATE TABLE group (
  id INT PRIMARY KEY AUTO_INCREMENT,
  name VARCHAR(255) NOT NULL,
  created_date TIMESTAMP NOT NULL,
  modified_date TIMESTAMP NOT NULL
);

CREATE TABLE resource (
  id INT PRIMARY KEY AUTO_INCREMENT,
  name VARCHAR(255) NOT NULL,
  group_id INT,
  FOREIGN KEY (group_id) references group(id)
);

CREATE TABLE token (
   id INT PRIMARY KEY AUTO_INCREMENT,
   created_date TIMESTAMP NOT NULL,
   modified_date TIMESTAMP,
   token VARCHAR(255) NOT NULL,
   group_id INT NOT NULL,
   FOREIGN KEY (group_id) references group(id)
 );

INSERT INTO group VALUES
    (1, "test-group", "2018-11-29 00:00:00", "2018-11-29 00:00:00");

INSERT INTO resource VALUES
    (1, "resourceA", 1),
    (1, "resourcB", 1);

INSERT INTO token VALUES
    (1, "2018-11-29 00:00:00", "2018-11-29 00:00:00", "test-token", 1);
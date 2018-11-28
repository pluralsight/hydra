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
  FOREIGN KEY (group_id) references groups(id)
);

CREATE TABLE token (
   id INT PRIMARY KEY AUTO_INCREMENT,
   created_date TIMESTAMP NOT NULL,
   modified_date TIMESTAMP,
   token VARCHAR(255) NOT NULL,
   group_id INT NOT NULL,
   FOREIGN KEY (group_id) references groups(id)
 );
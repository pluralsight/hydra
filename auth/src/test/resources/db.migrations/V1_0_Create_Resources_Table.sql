CREATE TABLE resources (
  id INT PRIMARY KEY AUTO_INCREMENT,
  name VARCHAR(255) NOT NULL,
  group_id INT,
  FOREIGN KEY (group_id) references groups(id)
)
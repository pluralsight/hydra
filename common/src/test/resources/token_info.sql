CREATE TABLE token_info (
   token TEXT PRIMARY KEY,
   insert_date TIMESTAMP NOT NULL,
   groups TEXT[] NOT NULL
 )
-- MySQL runs this automatically only when grammys_db_data is initialized.
-- The CSV is loaded into grammys.awards (not into a separate grammys_src DB).

CREATE DATABASE IF NOT EXISTS grammys;
USE grammys;

CREATE TABLE IF NOT EXISTS `awards` (
  `year` int DEFAULT NULL,
  `title` text,
  `published_at` text,
  `updated_at` text,
  `category` text,
  `nominee` text,
  `artist` text,
  `workers` text,
  `img` text,
  `winner` text
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Re-running this script reloads the CSV snapshot instead of appending duplicate rows.
TRUNCATE TABLE `awards`;

LOAD DATA INFILE '/seed/the_grammy_awards.csv'
INTO TABLE awards
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
ESCAPED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(`year`, `title`, `published_at`, `updated_at`, `category`, `nominee`,
 `artist`, `workers`, `img`, `winner`);
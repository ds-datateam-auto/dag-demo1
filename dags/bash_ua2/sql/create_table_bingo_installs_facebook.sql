DROP TABLE IF EXISTS bash.bingo_installs_facebook_ods;
CREATE TABLE IF NOT EXISTS bash.bingo_installs_facebook_ods (
  id NUMBER,
  campaign_id VARCHAR(100),
  campaign_name VARCHAR(100),
  creative_id VARCHAR(100),
  creative_name VARCHAR(100),
  adset_id VARCHAR(100),
  adset_name VARCHAR(100)
);

ALTER TABLE bash.bingo_installs_facebook_ods OWNER TO airflow;
DROP TABLE trnd_metric_val;

CREATE TABLE trnd_metric_val
(PRIMARY KEY trnd_metric_val_id INT NOT NULL,
             trnd_metric_id INT NOT NULL,
             value INT NOT NULL,
             day_key DATE NOT NULL,
             insert_tsp TIMESTAMP NOT NULL,
             insert_user VARCHAR(100) NOT NULL) ENGINE = INNODB;

ALTER TABLE trnd_metric_val ADD UNIQUE INDEX(trnd_metric_id, day_key);

CREATE TABLE trnd_metric
(PRIMARY KEY trnd_metric_id INT NOT NULL,
             metric_name VARCHAR(200) NOT NULL,
             description VARCHAR(1000),
             insert_tsp TIMESTAMP NOT NULL,
             insert_user VARCHAR(100) NOT NULL,
             update_tsp TIMESTAMP,
             update_user VARCHAR(100)) ENGINE = INNODB; 

ALTER TABLE trnd_metric ADD UNIQUE INDEX(metric_name);

CREATE TABLE trnd_metric_f
(PRIMARY KEY trnd_metric_f_id INT NOT NULL,
             trnd_metric_id INT NOT NULL,
             value INT NOT NULL,
             last_day_value INT,
             last_day_date DATE,
             last_week_avg INT,
             last_week_date DATE,
             last_month_avg INT,
             last_month_date DATE,
             last_year_avg INT,
             last_year_date DATE,
             rolling_7_day INT,
             rolling_7_day_date DATE,
             rolling_30_day INT,
             rolling_30_day_date DATE,
             rolling_12_mnt INT,
             rolling_12_mnt_date DATE,
             day_key DATE,
             insert_tsp TIMESTAMP,
             insert_user VARCHAR(100)) ENGINE = INNODB;

ALTER TABLE trnd_metric_f ADD UNIQUE INDEX(trnd_metric_id, day_key);

CREATE TABLE trnd_rule
(PRIMARY KEY trnd_rule_id INT NOT NULL,
             rule_name VARCHAR(200) NOT NULL,
             code TEXT,
             description VARCHAR(1000),
             insert_tsp TIMESTAMP NOT NULL,
             insert_user VARCHAR(100) NOT NULL,
             update_tsp TIMESTAMP,
             update_user VARCHAR(100)) ENGINE = INNODB;

ALTER TABLE trnd_rule ADD UNIQUE INDEX(rule_name);
ALTER TABLE tren_rule ADD UNIQUE INDEX(code);

CREATE TABLE trnd_rule_res
(PRIMARY KEY trnd_rule_res_id INT NOT NULL,
             trnd_rule_id INT NOT NULL,
             trnd_metric_f_id INT NOT NULL,
             rule_percent_diviation INT NOT NULL,
             diviation_direction VARCHAR(1) NOT NULL,
             insert_tsp TIMESTAMP NOT NULL,
             insert_user TIMESTAMP NOT NULL) ENGINE = INNODB;

ALTER TABLE trnd_rule_res ADD UNIQUE INDEX(trnd_rule_id, trnd_metric_f_id);

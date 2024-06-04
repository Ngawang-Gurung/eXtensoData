USE customer;

-- Creating Config Table--

SELECT total_value_chain_count
FROM new_customer_profile;

SELECT total_value_chain_count
FROM new_customer_profile
WHERE total_value_chain_count > 100;

CREATE TABLE config
(
    config_id  INT PRIMARY KEY AUTO_INCREMENT,
    config_key VARCHAR(255) NOT NULL,
    value      VARCHAR(255) NOT NULL
);

INSERT INTO config(config_key, value)
VALUES ('vc_count', 100);


SELECT *
FROM new_customer_profile
WHERE total_value_chain_count > (SELECT config.value
                                 FROM config
                                 WHERE config.config_key = 'vc_count');
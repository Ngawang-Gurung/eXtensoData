-- Trigger in MySQL --

USE mydb;

SHOW TABLES;

CREATE TABLE items
(
    id    INT PRIMARY KEY,
    name  VARCHAR(255)   NOT NULL,
    price DECIMAL(10, 2) NOT NULL
);

INSERT INTO items(id, name, price)
VALUES (1, 'Item', 50.00);

CREATE TABLE item_changes
(
    change_id        INT PRIMARY KEY AUTO_INCREMENT,
    item_id          INT,
    change_type      VARCHAR(10),
    change_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (item_id) REFERENCES items (id)
);

DELIMITER //

CREATE TRIGGER update_items_trigger
    AFTER UPDATE
    ON items
    FOR EACH ROW
BEGIN
INSERT INTO item_changes(item_id, change_type)
VALUES (NEW.id, 'UPDATE');
END //
DELIMITER ;

UPDATE items
SET price = 70.00
WHERE id = 1;

SELECT * FROM item_changes;

DELIMITER //
CREATE TRIGGER insert_items_trigger
    AFTER INSERT
    ON items
    FOR EACH ROW
BEGIN
INSERT INTO item_changes(item_id, change_type)
VALUES (NEW.id, 'Insert');
END //
DELIMITER ;

INSERT INTO items(id, name, price) VALUES (100, 'MyItem', 5000);

SHOW TRIGGERS ;

-- Event in MySQL, Also known as Temporal Trigger--

SHOW PROCESSLIST ; # See if there is event_scheduler if not SET GLOBAL event_scheduler = ON;

DROP TABLE IF EXISTS messages;
CREATE TABLE IF NOT EXISTS messages (
    id INT AUTO_INCREMENT PRIMARY KEY,
    message VARCHAR(255) NOT NULL ,
    created_at DATETIME DEFAULT NOW()
);

DROP EVENT IF EXISTS one_time_log;
CREATE EVENT IF NOT EXISTS one_tme_log
ON SCHEDULE AT CURRENT_TIMESTAMP
DO
INSERT INTO messages(message) VALUES ('one-time-event');

SELECT * FROM messages;

SHOW EVENTS FROM mydb;

-- Preserved Event --
CREATE EVENT one_time_log
ON SCHEDULE AT CURRENT_TIMESTAMP + INTERVAL 10 SECOND
ON COMPLETION PRESERVE
DO
   INSERT INTO messages(message)
   VALUES('Preserved One-time event');

SHOW EVENTS FROM mydb;

-- Recurring Event --

CREATE EVENT reccuring_log
ON SCHEDULE EVERY 10 SECOND
STARTS current_timestamp
ENDS current_timestamp + INTERVAL 1 MINUTE
DO
INSERT INTO messages(message)
VALUES (CONCAT('Running at ', NOW()));

SELECT * FROM messages;



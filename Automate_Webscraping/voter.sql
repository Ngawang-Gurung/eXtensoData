SHOW DATABASES;
USE voter;

show tables;

describe gandaki;

select COUNT(*) from gandaki;

select * from gandaki where `मतदाता नं` = '24254855';

create index voter_id 
on gandaki (`मतदाता नं`(100));

select * from gandaki where `मतदाता नं` = '24254855';
USE edw;

create table if not exists product_dim (
id integer NOT NULL auto_increment,
name varchar(255),
primary key(id)
);

create table if not exists customer_dim (
id integer NOT NULL auto_increment,
name varchar(255),
primary key(id)
);

create table if not exists date_dim (
id integer NOT NULL auto_increment,
name varchar(255),
primary key(id)
);

create table if not exists location_dim (
id integer NOT NULL auto_increment,
name varchar(255),
primary key(id)
);

create table if not exists consolidated_sales_fact (

id integer NOT NULL auto_increment,
product_key integer NOT NULL,
customer_key integer NOT NULL,
date_key integer NOT NULL,
location_key integer NOT NULL,

primary key(id),
constraint fk_product_key foreign key (product_key) references product_dim (id),
constraint fk_customer_key foreign key (customer_key) references customer_dim (id),
constraint fk_date_key foreign key (date_key) references date_dim (id),
constraint fk_location_key foreign key (location_key) references location_dim (id)
);

--------------------------------------------СТЕЙДЖИНГ
create table de11an.kmdd_stg_transactions (
	trans_id varchar,
	trans_date date,
	amt varchar,
	card_num varchar,
	oper_type varchar,
	oper_result varchar,
	terminal varchar;

create table de11an.kmdd_stg_cards (
	card_num varchar,
	account_num varchar,
	create_dt date,
	update_dt date );

create table de11an.kmdd_stg_accounts (
	account_num varchar,
	valid_to date,
	client varchar,
	create_dt date,
	update_dt date );

create table de11an.kmdd_stg_clients (
	client_id varchar,
	last_name varchar,
	first_name varchar,
	patronymic varchar,
	date_of_birth date,
	passport_num varchar,
	passport_valid_to date,
	phone varchar,
	create_dt date,
	update_dt date );

create table de11an.kmdd_stg_terminals (
	terminal_id varchar,
	terminal_type varchar,
	terminal_city varchar,
	terminal_address varchar,
	update_dt date );

create table de11an.kmdd_stg_blacklist (
	passport_num varchar,
	entry_dt date);


----------------------------------------------DWH

create table de11an.kmdd_dwh_fact_transactions (
	trans_id varchar,
	trans_date date,
	card_num varchar ,
	oper_type varchar,
	amt varchar,
	oper_result varchar,
	terminal varchar);


create table de11an.kmdd_dwh_dim_cards (
	rard_num varchar,
	account_num varchar,
	create_dt date,
	update_dt date );

create table de11an.kmdd_dwh_dim_accounts (
	account_num varchar,
	valid_to date,
	client varchar,
	create_dt date,
	update_dt date );

create table de11an.kmdd_dwh_dim_clients (
	client_id varchar,
	last_name varchar,
	first_name varchar,
	patronymic varchar,
	date_of_birth date,
	passport_num varchar,
	passport_valid_to date,
	phone varchar,
	create_dt date,
	update_dt date );

create table de11an.kmdd_dwh_dim_terminals (
	terminal_id varchar,
	terminal_type varchar,
	terminal_city varchar,
	terminal_address varchar,
	create_dt date,
	update_dt date );

create table de11an.kmdd_dwh_fact_passport_blacklist (
	passport_num varchar,
	entry_dt date);


create table de11an.kmdd_rep_fruad (
	event_id date,
	passport varchar,
	fio varchar,
	phone varchar,
	event_type varchar,
	report_dt date);
----------------------------------------------------


--create database analytics_dashboard;

create schema if not exists app_schema;

set search_path to app_schema;

create table users (
    user_id serial primary key,
    first_name varchar(50) not null,
    last_name varchar(50) not null,
    email varchar(255) unique not null,
    created_dttm timestamp with time zone default now()
);

create table products (
    product_id serial primary key,
    product_name varchar(255) not null,
    price decimal(10, 2) not null,
    created_dttm timestamp with time zone default now()
);

create table transactions (
    transaction_id serial primary key,
    user_id integer references users(user_id) on delete cascade,
    product_id integer references products(product_id) on delete cascade,
    amount decimal(10, 2) not null,
    transaction_status varchar(50) not null,
    created_dttm timestamp with time zone default now()
);

create table user_actions (
    action_id serial primary key,
    user_id integer references users(user_id) delete on cascade,
    action_type varchar(255) not null,
    created_dttm timestamp with time zone default now()
);

create index idx_transactions_user_id on transactions(user_id);
create index idx_transactions_created_dttm on transactions(created_dttm);
create index idx_transactions_transaction_status on transactions(transaction_status);
create index idx_user_actions_user_id on user_actions(user_id);
create index idx_user_actions_created_dttm on user_actions(created_dttm);
create index idx_user_actions_action_type on user_actions(action_type);
create index idx_users_created_dttm on users(created_dttm);

create view successful_transactions as
    select t.*, u.email as user_email,
           p.product_name as product_name
    from transactions t
    join users u on t.user_id = u.user_id
    join products p on t.product_id = p.product_id
    where t.transaction_status = 'success';

select 'database, schema, tables, indexes and views created successfully!' as status;
-- Your SQL goes here
create table event (
    id serial primary key,
    summary varchar not null,
    location varchar not null,
    describe varchar not null
);

create table attendes (
    id serial primary key,
    email varchar not null unique,
    idevent integer references event not null
);

create table endl (
    id serial primary key,
    dt timestamp without time zone not null,
    timezone varchar not null,
    idevent integer references event not null
);

create table reminders (
    id serial primary key,
    usedefault boolean, 
    idevent integer references event not null
);

create table recurrence (
    id serial primary key,
    rrule varchar not null,
    idevent integer references event not null
);

create table start (
    id serial primary key,
    dt timestamp without time zone not null,
    timezone varchar not null,
    idevent integer references event not null
);

create table overrides (
    id serial primary key,
    method varchar not null unique,
    minutes integer not null,
    idreminders integer references reminders not null,
    idevent integer references event not null
);
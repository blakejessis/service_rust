// @generated automatically by Diesel CLI.

diesel::table! {
    attendes (id) {
        id -> Int4,
        email -> Varchar,
        idevent -> Nullable<Int4>,
    }
}

diesel::table! {
    endl (id) {
        id -> Int4,
        datetime -> Timestamp,
        timezone -> Varchar,
        idevent -> Nullable<Int4>,
    }
}

diesel::table! {
    event (id) {
        id -> Int4,
        summary -> Varchar,
        location -> Varchar,
        description -> Nullable<Varchar>,
    }
}

diesel::table! {
    overrides (id) {
        id -> Int4,
        method -> Varchar,
        minutes -> Int4,
        idreminders -> Nullable<Int4>,
        idevent -> Nullable<Int4>,
    }
}

diesel::table! {
    recurrence (id) {
        id -> Int4,
        rrule -> Varchar,
        idevent -> Nullable<Int4>,
    }
}

diesel::table! {
    reminders (id) {
        id -> Int4,
        usedefault -> Bool,
        idevent -> Nullable<Int4>,
    }
}

diesel::table! {
    start (id) {
        id -> Int4,
        datetime -> Timestamp,
        timezone -> Varchar,
        idevent -> Nullable<Int4>,
    }
}

diesel::joinable!(attendes -> event (idevent));
diesel::joinable!(endl -> event (idevent));
diesel::joinable!(overrides -> event (idevent));
diesel::joinable!(overrides -> reminders (idreminders));
diesel::joinable!(recurrence -> event (idevent));
diesel::joinable!(reminders -> event (idevent));
diesel::joinable!(start -> event (idevent));

diesel::allow_tables_to_appear_in_same_query!(
    attendes,
    endl,
    event,
    overrides,
    recurrence,
    reminders,
    start,
);

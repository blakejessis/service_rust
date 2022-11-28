use diesel::prelude::*;

use crate::persistence::models::{Attende,
    Endl, Events, Override, Recurrence,
    Reminder, Start, NewAttende, NewEndl,
    NewEvent, NewOverride, NewRecurrence,
    NewReminder, NewStart
};
use crate::persistence::schema::{attendes, endl,
    event, overrides, recurrence, reminders, start};

pub fn get_all(conn: &mut PgConnection) -> QueryResult<Vec<Events>> {
    use crate::persistence::schema::event::dsl::*;

    planets.load(conn)
}

pub fn get(id: i32, conn: &mut PgConnection) -> QueryResult<Events> {
    planets::table.find(id).get_result(conn)
}

pub fn get_attendes(event_ids: &[i32], conn: &mut PgConnection) -> QueryResult<Vec<Attende>> {
    attendes::table
        .filter(attendes::idevent.eq_any(event_ids))
        .load::<Attende>(conn)
}

pub fn get_end(event_ids: &[i32], conn: &mut PgConnection) -> QueryResult<Vec<Endl>> {
    endl::table
        .filter(endl::idevent.eq_any(event_ids))
        .load::<Endl>(conn)
}

pub fn get_overrides(event_ids: &[i32], conn: &mut PgConnection) -> QueryResult<Vec<Override>> {
    overrides::table
        .filter(overrides::idevent.eq_any(event_ids))
        .load::<Override>(conn)
}

pub fn get_recurrence(event_ids: &[i32], conn: &mut PgConnection) -> QueryResult<Vec<Recurrence>> {
    reccurence::table
        .filter(recurrence::idevent.eq_any(event_ids))
        .load::<Recurrence>(conn)
}

pub fn get_reminders(event_ids: &[i32], conn: &mut PgConnection) -> QueryResult<Vec<Reminder>> {
    reminders::table
        .filter(reminders::idevent.eq_any(event_ids))
        .load::<Reminder>(conn)
}

pub fn get_start(event_ids: &[i32], conn: &mut PgConnection) -> QueryResult<Vec<Start>> {
    start::table
        .filter(start::idevent.eq_any(event_ids))
        .load::<Start>(conn)
}



pub fn create(
    new_event: NewEvent,
    mut new_attendes: NewAttende,
    mut new_endl: NewEndl,
    mut new_overrides: NewOverride,
    mut new_recurrence: NewRecurrence,
    mut new_reminder: NewReminder,
    mut new_start: NewStart,
    conn: &mut PgConnection,
) -> QueryResult<Events> {
    use crate::persistence::schema::{endl::dsl::*, event::dsl::*, attendes::dsl::*,
        overrides::dsl::*, recurrence::dsl::*, reminder::dsl::*, start::dsl::*};

    let created_event: Events = diesel::insert_into(event)
        .values(new_event)
        .get_result(conn)?;

    new_attendes.idevent = Some(created_event.id);

    diesel::insert_into(attendes)
        .values(new_attendes)
        .execute(conn)?;

    new_endl.idevent = Some(created_event.id);

    diesel::insert_into(endl)
        .values(new_endl)
        .execute(conn)?;

    new_overrides.idevent = Some(created_event.id);

    diesel::insert_into(overrides)
        .values(new_overrides)
        .execute(conn)?;
    
    new_recurrence.idevent = Some(created_event.id);

    diesel::insert_into(recurrence)
        .values(new_recurrence)
        .execute(conn)?;

    new_reminder.idevent = Some(created_event.id);

    diesel::insert_into(reminders)
        .values(new_reminder)
        .execute(conn)?;
    
    new_start.idevent = Some(created_event.id);

    diesel::insert_into(start)
        .values(new_start)
        .execute(conn)?;
    

    Ok(created_event)
}
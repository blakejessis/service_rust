#![recursion_limit="512"]
#[macro_use]
extern crate diesel;
extern crate dotenv;

use diesel::prelude::*;
use diesel::pg::PgConnection;
use dotenv::dotenv;
use std::env;

mod models;
mod schema;

use models::Event;
use schema::event::dsl::*;

fn main() {
    dotenv().ok();
    let data_url = env::var("DATABASE_URL")
        .expect("DATABASE_URL must be set");
    let connection = &mut PgConnection::establish(&data_url)
        .expect(&format!("Error connect to {}", data_url));

    let results = event.limit(2)
        .load::<Event>(connection)
        .expect("Error loading events");

    // for events in results {
    //     print!("{} \n", event.id);
    // }
}


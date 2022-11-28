use std::collections::HashMap;
use std::env;
use std::fmt::{self, Formatter, LowerExp};
use std::iter::Iterator;
use std::str::FromStr;
use std::sync::{Arc, Mutex};

use async_graphql::dataloader::{DataLoader, Loader};
use async_graphql::*;
use bigdecimal::{BigDecimal, ToPrimitive};
use futures::{Stream, StreamExt};
use rdkafka::{producer::FutureProducer, Message};
use serde::{Deserialize, Serialize};
use strum_macros::{Display, EnumString};

use common_utils::{CustomError, Role, FORBIDDEN_MESSAGE};

use crate::get_conn_from_ctx;
use crate::kafka;
use crate::persistence::connection::PgPool;
use crate::persistence::models::{Attende,
    Endl, Events, Override, Recurrence,
    Reminder, Start, NewAttende, NewEndl,
    NewEvent, NewOverride, NewRecurrence,
    NewReminder, NewStart
};
use crate::persistence::repository;

pub type AppSchema = Schema<Query, Mutation, Subscription>;

pub struct Query;

#[Object]
impl Query {
    async fn get_events(&self, ctx: &Context<'_>) -> Vec<Events> {
        repository::get_all(&mut get_conn_from_ctx(ctx))
            .expect("Can't get events")
            .iter()
            .map(Events::from)
            .collect()
    }

    async fn get_event(&self, ctx: &Context<'_>, id: ID) -> Option<Events> {
        find_event_by_id_internal(ctx, id)
    }

    #[graphql(entity)]
    async fn find_event_by_id(&self, ctx: &Context<'_>, id: ID) -> Option<Events> {
        find_event_by_id_internal(ctx, id)
    }
}

fn find_event_by_id_internal(ctx: &Context<'_>, id: ID) -> Option<Events> {
    let id = id
        .to_string()
        .parse::<i32>()
        .expect("Can't get id from String");
    repository::get(id, &mut get_conn_from_ctx(ctx))
        .ok()
        .map(|p| Events::from(&p))
}

pub struct Mutation;

#[Object]
impl Mutation {
    #[graphql(guard = "RoleGuard::new(Role::Admin)")]
    async fn create_event(&self, ctx: &Context<'_>, event: EventInput) -> Result<Event> {
        let new_event = NewEvent {
            summary: event.summary,
            location: event.location,
            description: event.description,
        };

        let attendes = event.attendes;
        let new_event_attendes = NewAttende {
            email: attendes.email,
            idevent: 0,
        };

        let endl = event.endl;
        let new_event_endl = NewEndl {
            datetime: endl.datetime.to_string(),
            timezone: endl.timezone.to_string(),
            idevent: 0,
        };
        
        let overrides = event.overrides;
        let new_event_overrides = NewOverride {
            method: overrides.method.to_string(),
            minutes: overrides.minutes.to_string(),
            idreminders: 0,
            idevent: 0,
        };

        let recurrence = event.recurrence;
        let new_event_recurrence = NewRecurrence {
            rrule: recurrence.rrule,
            idevent: 0,
        };

        let reminders = event.reminders;
        let new_event_reminders = NewReminder {
            usedefault: reminders.usedefault.to_string(),
            idevent: 0,
        };

        let start = event.start;
        let new_event_start = NewStart {
            datetime: start.datetime.to_string(),
            timezone: start.timezone.to_string(),
            idevent: 0,
        };

        let created_event =
            repository::create(new_event, new_event_attendes, new_event_endl,
            new_event_overrides, new_event_recurrence, new_event_reminders,
            new_event_start, &mut get_conn_from_ctx(ctx))?;

        let producer = ctx
            .data::<FutureProducer>()
            .expect("Can't get Kafka producer");
        let message = serde_json::to_string(&Events::from(&created_event))
            .expect("Can't serialize a event");
        kafka::send_message(producer, &message).await;

        Ok(Events::from(&created_event))
    }
}

pub struct Subscription;

#[Subscription]
impl Subscription {
    async fn latest_event<'ctx>(
        &self,
        ctx: &'ctx Context<'_>,
    ) -> impl Stream<Item = NewEvent> + 'ctx {
        let kafka_consumer_counter = ctx
            .data::<Mutex<i32>>()
            .expect("Can't get Kafka consumer counter");
        let consumer_group_id = kafka::get_kafka_consumer_group_id(kafka_consumer_counter);
        // In fact, there should be only one Kafka consumer in this application. It should broadcast
        // messages from a topic to each subscriber. For simplicity purposes a consumer is created per
        // each subscription
        let consumer = kafka::create_consumer(consumer_group_id);

        async_stream::stream! {
            let mut stream = consumer.stream();

            while let Some(value) = stream.next().await {
                yield match value {
                    Ok(message) => {
                        let payload = message.payload().expect("Kafka message should contain payload");
                        let message = String::from_utf8_lossy(payload).to_string();
                        serde_json::from_str(&message).expect("Can't deserialize a event")
                    }
                    Err(e) => panic!("Error while Kafka message processing: {}", e)
                };
            }
        }
    }
}

#[derive(Serialize, Deserialize)]
struct Event {
    id: ID,
    summary: String,
    description: String,
}

#[Object]
impl Event {
    async fn id(&self) -> &ID {
        &self.id
    }

    async fn summary(&self) -> &String {
        &self.summary
    }

    async fn description(&self) -> &String {
        &self.description
    }


    async fn attendes(&self, ctx: &Context<'_>) -> Result<Attende> {
        let data_loader = ctx
            .data::<DataLoader<AttendesLoader>>()
            .expect("Can't get data loader");
        let idevent = self
            .id
            .to_string()
            .parse::<i32>()
            .expect("Can't convert id");
        let attendes = data_loader.load_one(idevent).await?;
        attendes.ok_or_else(|| "Not found".into())
    }

    async fn endl(&self, ctx: &Context<'_>) -> Result<Endl> {
        let data_loader = ctx
            .data::<DataLoader<EndlLoader>>()
            .expect("Can't get data loader");
        let idevent = self
            .id
            .to_string()
            .parse::<i32>()
            .expect("Can't convert id");
        let endl = data_loader.load_one(idevent).await?;
        endl.ok_or_else(|| "Not found".into())
    }

    async fn overrides(&self, ctx: &Context<'_>) -> Result<Override> {
        let data_loader = ctx
            .data::<DataLoader<OverrideLoader>>()
            .expect("Can't get data loader");
        let idevent = self
            .id
            .to_string()
            .parse::<i32>()
            .expect("Can't convert id");
        let overrides = data_loader.load_one(idevent).await?;
        overrides.ok_or_else(|| "Not found".into())
    }

    async fn recurrence(&self, ctx: &Context<'_>) -> Result<Recurrence> {
        let data_loader = ctx
            .data::<DataLoader<RecurrenceLoader>>()
            .expect("Can't get data loader");
        let idevent = self
            .id
            .to_string()
            .parse::<i32>()
            .expect("Can't convert id");
        let recurrence = data_loader.load_one(idevent).await?;
        recurrence.ok_or_else(|| "Not found".into())
    }

    async fn reminders(&self, ctx: &Context<'_>) -> Result<Reminder> {
        let data_loader = ctx
            .data::<DataLoader<ReminderLoader>>()
            .expect("Can't get data loader");
        let idevent = self
            .id
            .to_string()
            .parse::<i32>()
            .expect("Can't convert id");
        let reminders = data_loader.load_one(idevent).await?;
        reminders.ok_or_else(|| "Not found".into())
    }

    async fn start(&self, ctx: &Context<'_>) -> Result<Start> {
        let data_loader = ctx
            .data::<DataLoader<StartLoader>>()
            .expect("Can't get data loader");
        let idevent = self
            .id
            .to_string()
            .parse::<i32>()
            .expect("Can't convert id");
        let start = data_loader.load_one(idevent).await?;
        start.ok_or_else(|| "Not found".into())
    }
    
}

// #[derive(Copy, Clone, Eq, PartialEq, Serialize, Deserialize, Enum, Display, EnumString)]
// #[strum(serialize_all = "SCREAMING_SNAKE_CASE")]


// #[derive(Interface, Clone)]
// #[graphql(
//     field(name = "mean_radius", type = "&CustomBigDecimal"),
//     field(name = "mass", type = "&CustomBigInt")
// )]
// pub enum Details {
//     InhabitedPlanetDetails(InhabitedPlanetDetails),
//     UninhabitedPlanetDetails(UninhabitedPlanetDetails),
// }

// #[derive(SimpleObject, Clone)]
// pub struct InhabitedPlanetDetails {
//     mean_radius: CustomBigDecimal,
//     mass: CustomBigInt,
//     /// In billions
//     population: CustomBigDecimal,
// }

// #[derive(SimpleObject, Clone)]
// pub struct UninhabitedPlanetDetails {
//     mean_radius: CustomBigDecimal,
//     mass: CustomBigInt,
// }

// #[derive(Clone)]
// pub struct CustomBigInt(BigDecimal);

// #[Scalar(name = "BigInt")]
// impl ScalarType for CustomBigInt {
//     fn parse(value: Value) -> InputValueResult<Self> {
//         match value {
//             Value::String(s) => {
//                 let parsed_value = BigDecimal::from_str(&s)?;
//                 Ok(CustomBigInt(parsed_value))
//             }
//             _ => Err(InputValueError::expected_type(value)),
//         }
//     }

//     fn to_value(&self) -> Value {
//         Value::String(format!("{:e}", &self))
//     }
// }

// impl LowerExp for CustomBigInt {
//     fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
//         let val = &self.0.to_f64().expect("Can't convert BigDecimal");
//         LowerExp::fmt(val, f)
//     }
// }

// #[derive(Clone)]
// pub struct CustomBigDecimal(BigDecimal);

// #[Scalar(name = "BigDecimal")]
// impl ScalarType for CustomBigDecimal {
//     fn parse(value: Value) -> InputValueResult<Self> {
//         match value {
//             Value::String(s) => {
//                 let parsed_value = BigDecimal::from_str(&s)?;
//                 Ok(CustomBigDecimal(parsed_value))
//             }
//             _ => Err(InputValueError::expected_type(value)),
//         }
//     }

//     fn to_value(&self) -> Value {
//         Value::String(self.0.to_string())
//     }
// }

#[derive(InputObject)]
struct EventInput {
    summary: String,
    description: String,
    // type_: PlanetType,
    // details: DetailsInput,
}

#[derive(InputObject)]
struct AttendesInput {
    pub email: String,
}

#[derive(InputObject)]
struct EndlInput {
    pub datetime: NaiveDateTime,
    pub timezone: String,
}

#[derive(InputObject)]
struct OverridesInput {
    pub method: String,
    pub minutes: i32,
    pub idreminders: RemindersInput,
}

#[derive(InputObject)]
struct RecurrenceInput {
    pub rrule: String,    
}

#[derive(InputObject)]
struct RemindersInput {
    pub usedefault: bool,
}

#[derive(InputObject)]
struct StartInput {
    pub datetime: NaiveDateTime,
    pub timezone: String,
}

impl From<&Events> for Event {
    fn from(entity: &Events) -> Self {
        Event {
            id: entity.id.into(),
            summary: entity.summary.clone(),
            description: entity.description.clone(),
        }
    }
}

impl From<&Attende> for Attendes {
    fn from(entity: &Attende) -> Self {
        Attendes {
            email: string,
        }
    }
}

pub struct AttendesLoader {
    pub pool: Arc<PgPool>,
}

#[async_trait::async_trait]
impl Loader<i32> for AttendesLoader {
    type Value = Attendes;
    type Error = Error;

    async fn load(&self, keys: &[i32]) -> Result<HashMap<i32, Self::Value>, Self::Error> {
        let mut conn = self.pool.get()?;
        let attendes = repository::get_attendes(keys, &mut conn)?;

        Ok(attendes
            .iter()
            .map(|attendes_entity| (attendes_entity.idevent, Attendes::from(attendes_entity)))
            .collect::<HashMap<_, _>>())
    }
}

// struct RoleGuard {
//     role: Role,
// }

// impl RoleGuard {
//     fn new(role: Role) -> Self {
//         Self { role }
//     }
// }

// #[async_trait::async_trait]
// impl Guard for RoleGuard {
//     async fn check(&self, ctx: &Context<'_>) -> Result<()> {
//         // TODO: auth disabling is needed for tests. try to reimplement when https://github.com/rust-lang/rust/issues/45599 will be resolved (using cfg(test))
//         if let Ok(boolean) = env::var("DISABLE_AUTH") {
//             let disable_auth = bool::from_str(boolean.as_str()).expect("Can't parse bool");
//             if disable_auth {
//                 return Ok(());
//             }
//         };

//         let maybe_getting_role_result = ctx.data_opt::<Result<Option<Role>, CustomError>>();
//         match maybe_getting_role_result {
//             Some(getting_role_result) => {
//                 let check_role_result =
//                     common_utils::check_user_role_is_allowed(getting_role_result, &self.role);
//                 match check_role_result {
//                     Ok(_) => Ok(()),
//                     Err(e) => Err(Error::new(e.message)),
//                 }
//             }
//             None => Err(FORBIDDEN_MESSAGE.into()),
//         }
//     }
// }
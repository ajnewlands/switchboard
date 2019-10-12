use actix::prelude::*;
use uuid::Uuid;
use env_logger;
use lapin;
use log::{info, error};
use std::sync::mpsc;
use std::thread;
use std::time::Duration;
use std::error::Error;

use lapin::{
  BasicProperties, Channel, Connection, ConnectionProperties, ConsumerDelegate,
  message::Delivery,
  options::*,
  types::FieldTable,
};

pub fn get_connection(amqp: String, timeout: u64) -> Result<Connection, String> {
        let (sender, receiver) = mpsc::channel();

        info!("Connecting to rabbitmq");

        let t = thread::spawn(move ||{
            let conn = Connection::connect(&amqp, ConnectionProperties::default())
                .wait()
                .expect("Failed connecting to rabbit");
            match sender.send(conn) {
                Ok(()) => info!("Connected to rabbit"),
                Err(_) => {},
            };
        });

        return match receiver.recv_timeout(Duration::from_millis(timeout)) {
            Ok(c) => Ok(c),
            Err(_) => Err(String::from("Timed out connecting to rabbit")), 
        };
}

pub struct RabbitReceiver {
    conn: Connection,
    count: usize,
}

impl RabbitReceiver {
    pub fn with_connection(conn: Connection) -> RabbitReceiver {
        return RabbitReceiver{ conn: conn, count: 0};
    }
}

impl Handler<Add> for RabbitReceiver {
    type Result = ();

    fn handle(&mut self, _msg: Add, _ctx: &mut Context<Self>)  {
        self.count += 1;
        let session_id = Uuid::new_v4();
        println!("Session id {}", session_id);
        println!("Connected sockets now {}", self.count);
    }
}

impl Handler<Goodbye> for RabbitReceiver {
    type Result = ();

    fn handle (&mut self, _msg: Goodbye, _ctx: &mut Context<Self>)  {
        self.count -= 1;
        println!("Connected sockets now {}", self.count);

    }
}

// Rabbit receiver object
impl Actor for RabbitReceiver {
    type Context = Context<Self>;

    fn started(&mut self, _ctx: &mut Self::Context) {
        println!("Rabbit actor started");
    }
}

/// Message send to rabbit receiver to register another websocket on the switchboard.
#[derive(Clone, Debug, Message)]
pub struct Add {}

/// Message sent to Rabbit Receiver to deregister a websocket on disconnection.
#[derive(Clone, Debug, Message)]
pub struct Goodbye{}
use actix::prelude::*;
use uuid::Uuid;
use lapin;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use lapin::{
  BasicProperties, Channel, Connection, ConnectionProperties, ConsumerDelegate,
  message::Delivery,
  options::*,
  types::FieldTable,
};

pub fn get_connection(amqp: String, timeout: u64) -> Result<Connection, String> {
        let (sender, receiver) = mpsc::channel();

        let _t = thread::spawn(move ||{
            let conn = Connection::connect(&amqp, ConnectionProperties::default())
                .wait()
                .expect("Failed connecting to rabbit");
            sender.send(conn).unwrap();
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

impl Handler<AddSocket> for RabbitReceiver {
    type Result = ();

    fn handle(&mut self, _msg: AddSocket, _ctx: &mut Context<Self>)  {
        self.count += 1;
        let session_id = Uuid::new_v4();
        println!("Session id {}", session_id);
        println!("Connected sockets now {}", self.count);
    }
}

impl Handler<DelSocket> for RabbitReceiver {
    type Result = ();

    fn handle (&mut self, _msg: DelSocket, _ctx: &mut Context<Self>)  {
        self.count -= 1;
        println!("Connected sockets now {}", self.count);

    }
}

// Rabbit receiver object
impl Actor for RabbitReceiver {
    type Context = Context<Self>;
}

/// Message send to rabbit receiver to register another websocket on the switchboard.
#[derive(Clone, Debug, Message)]
pub struct AddSocket {}

/// Message sent to Rabbit Receiver to deregister a websocket on disconnection.
#[derive(Clone, Debug, Message)]
pub struct DelSocket{}
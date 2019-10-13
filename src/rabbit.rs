use actix::prelude::*;
use uuid::Uuid;
use lapin;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;
use std::rc::Rc;
use std::sync::Arc;


use lapin::{
  BasicProperties, Channel, Connection, ConnectionProperties, ConsumerDelegate,
  message::{Delivery, DeliveryResult},
  types::FieldTable,
  ExchangeKind, options::*,
  Queue,
};


pub struct RabbitReceiver {
    chan: Arc<Channel>,
    count: usize,
}

impl RabbitReceiver {
    pub fn new(amqp: String, timeout: u64, exchange: &str, queue: &str) -> Result<RabbitReceiver, std::io::Error> {
        let conn = RabbitReceiver::get_connection(amqp, timeout)?;
        let chan = Arc::new(RabbitReceiver::get_channel(conn)?);
        let ex = RabbitReceiver::create_exchange(chan.clone(), exchange)?;
        let q = RabbitReceiver::create_queue(chan.clone(), queue)?;
        RabbitReceiver::create_bindings(chan.clone(), queue, exchange)?;
        RabbitReceiver::consume(chan.clone(), &q)?;

        return Ok(RabbitReceiver{ chan: chan, count: 0});
    }

    /// Due to apparent deficiencies in Lapin, this won't return early when a connection is rejected.
    fn get_connection(amqp: String, timeout: u64) -> Result<Connection, std::io::Error> {
        let (sender, receiver) = mpsc::channel();

        let _t = thread::spawn(move ||{
            let conn = Connection::connect(&amqp, ConnectionProperties::default())
                .wait()
                .expect("Failed connecting to rabbit");
            sender.send(conn).unwrap();
            drop(sender);
        });

        return match receiver.recv_timeout(Duration::from_millis(timeout)) {
            Ok(conn) => Ok(conn),
            Err(e) => Err(std::io::Error::new(std::io::ErrorKind::NotConnected, e)), 
        };
    }

    /// Get a channel for the connection
    fn get_channel(conn: Connection) -> Result<Channel, std::io::Error> {
        return match conn.create_channel().wait() {
            Ok(ch) => Ok(ch),
            Err(e) => Err(std::io::Error::new(std::io::ErrorKind::NotConnected, e)),
        };
    }

    /// Declare the named exchange, creating it if it doesn exist already.
    fn create_exchange(chan: Arc<Channel>, exchange: &str) -> Result<&str, std::io::Error> {
        return match chan.exchange_declare(exchange , ExchangeKind::Headers, ExchangeDeclareOptions::default(), FieldTable::default()).wait() {
            Ok(_) => Ok(exchange),
            Err(e) => Err(std::io::Error::new(std::io::ErrorKind::NotConnected, e)),
        };
    }

    /// Declare the named queue (creating it if it doesn't exist)
    fn create_queue(chan: Arc<Channel>, queue: &str) -> Result<Queue, std::io::Error> {
        return match chan.queue_declare(queue, QueueDeclareOptions::default(), FieldTable::default()).wait() {
            Ok(q) => Ok(q),
            Err(e) => Err(std::io::Error::new(std::io::ErrorKind::NotConnected, e)),
        };
    }

    fn create_bindings(chan: Arc<Channel>, queue: &str, exchange: &str) -> Result<(), std::io::Error> {
        return match chan.queue_bind(queue, exchange, "", QueueBindOptions::default(), FieldTable::default()).wait() {
            Ok(_) => Ok(()),
            Err(e) => Err(std::io::Error::new(std::io::ErrorKind::NotConnected, e)),
        };
    }

    fn consume(chan: Arc<Channel>, queue: &Queue) -> Result<(), std::io::Error> {
        return match chan.basic_consume(queue, "switchboard", BasicConsumeOptions::default(), FieldTable::default()).wait() {
            Ok(con) => Ok(con.set_delegate(Box::new(move| delivery: DeliveryResult |{
                println!("received message: {:?}", delivery);
                if let Ok(Some(delivery)) = delivery {
                    chan.basic_ack(delivery.delivery_tag, BasicAckOptions::default()).wait().expect("ACK failed");
                };
            }))),
            Err(e) => Err(std::io::Error::new(std::io::ErrorKind::NotConnected, e)),
        };
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

/// Message sent to rabbit receiver to register another websocket on the switchboard.
#[derive(Clone, Debug, Message)]
pub struct AddSocket {}

/// Message sent to Rabbit Receiver to deregister a websocket on disconnection.
#[derive(Clone, Debug, Message)]
pub struct DelSocket{}
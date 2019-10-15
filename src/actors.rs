use uuid::Uuid;
use std::sync::{Arc, mpsc};
use std::thread;
use std::rc::Rc;
use std::time::{Duration, Instant};
use std::collections::HashMap;
use log::{info, warn, error};
use serde_json::json;

use actix::prelude::*;
use actix_web::web;
use actix_web_actors::ws;

use lapin::{
  Channel, Connection, ConnectionProperties, 
  message::DeliveryResult,
  types::{FieldTable, ShortString, AMQPValue, AMQPType},
  ExchangeKind, options::*,
  Queue,
};

/// How often heartbeat pings are sent
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);
/// How long before lack of client response causes a timeout
const CLIENT_TIMEOUT: Duration = Duration::from_secs(10);

pub struct RabbitReceiver {
    sessions: HashMap<Uuid, Addr<MyWebSocket>>,
}

impl RabbitReceiver {
    pub fn new(amqp: String, timeout: u64, exchange: &str, queue: &str) -> Result<RabbitReceiver, std::io::Error> {
        let conn = RabbitReceiver::get_connection(amqp, timeout)?;
        let chan = Arc::new(RabbitReceiver::get_channel(conn)?);
        let _ = RabbitReceiver::create_exchange(chan.clone(), exchange)?;
        let q = RabbitReceiver::create_queue(chan.clone(), queue)?;
        RabbitReceiver::create_bindings(chan.clone(), queue, exchange)?;
        RabbitReceiver::consume(chan.clone(), &q)?;

        return Ok(RabbitReceiver{ sessions: HashMap::with_capacity(64) });
    }

    /// Due to apparent deficiencies in Lapin, this won't return early when a connection is rejected.
    /// From experimentation, this seems to only be an issue on Windows (it will return immediately on Linux)
    fn get_connection(amqp: String, timeout: u64) -> Result<Connection, std::io::Error> {
        let (sender, receiver) = mpsc::channel();
        {
            let _t = thread::spawn(move ||{
                let conn = Connection::connect(&amqp, ConnectionProperties::default());
                match conn.wait() {
                    Ok(conn) => sender.send(conn).unwrap(),
                    Err(_) => drop(sender),
                };
            });
        }

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
        let mut fields = FieldTable::default();
        let j = json!("foo");
        let v = AMQPValue::try_from(&j, AMQPType::LongString).unwrap();
        fields.insert(ShortString::from("requestId"), v);
        return match chan.queue_bind(queue, exchange, "", QueueBindOptions::default(), fields).wait() {
            Ok(_) => Ok(()),
            Err(e) => Err(std::io::Error::new(std::io::ErrorKind::NotConnected, e)),
        };
    }

    fn consume(chan: Arc<Channel>, queue: &Queue) -> Result<(), std::io::Error> {
        return match chan.basic_consume(queue, "switchboard", BasicConsumeOptions::default(), FieldTable::default()).wait() {
            Ok(con) => Ok(con.set_delegate(Box::new(move| delivery: DeliveryResult |{
                match delivery {
                    Ok(Some(delivery)) => chan.basic_ack(delivery.delivery_tag, BasicAckOptions::default()).wait().expect("ACK failed"), // Got message
                    Ok(None) => info!("Consumer cancelled"), // Consumer cancelled
                    Err(e) => error!("Consumer error {}", e),
                };
            }))),
            Err(e) => Err(std::io::Error::new(std::io::ErrorKind::NotConnected, e)),
        };
    }
}

/// Each time a new websocket is created, store it's address and session id.
impl Handler<AddSocket> for RabbitReceiver {
    type Result = ();

    fn handle(&mut self, msg: AddSocket, _ctx: &mut Context<Self>)  {
        self.sessions.insert(msg.session_id, msg.sender);
        info!("Connected sockets now {}, added session {}", self.sessions.len(), msg.session_id);
    }
}

/// when a socket is closed, cease tracking it's session id and address.
impl Handler<DelSocket> for RabbitReceiver {
    type Result = ();

    fn handle (&mut self, msg: DelSocket, _ctx: &mut Context<Self>)  {
        match self.sessions.remove(&(msg.session_id)) {
            Some(_) => info!("Removed session {}, remaining sessions {}", msg.session_id, self.sessions.len()),
            None => warn!("Got disconnect for non-existent session {}", msg.session_id),
        };
    }
}

// Rabbit receiver object
impl Actor for RabbitReceiver {
    type Context = Context<Self>;

    fn stopped(&mut self, _ctx: &mut Self::Context) {
            info!("Stopped rabbit actor");
    }
}

/// Message sent to rabbit receiver to register another websocket on the switchboard.
#[derive(Clone, Message)]
pub struct AddSocket {
    sender: Addr<MyWebSocket>,
    session_id: Uuid,
}

/// Message sent to Rabbit Receiver to deregister a websocket on disconnection.
#[derive(Clone, Message)]
pub struct DelSocket{
    session_id: Uuid,
}

/// Actor implementing the websocket connection
pub struct MyWebSocket {
    /// Client must send ping at least once per 10 seconds (CLIENT_TIMEOUT),
    hb: Instant,
    rabbit: web::Data<Rc<Addr<RabbitReceiver>>>,
    id: Uuid,
}


impl Actor for MyWebSocket {
    type Context = ws::WebsocketContext<Self>;

    /// Method is called on actor start. We start the heartbeat process here.
    fn started(&mut self, ctx: &mut Self::Context) {
        self.rabbit.do_send(AddSocket{
            sender: ctx.address(), 
            session_id: self.id, 
        });

        self.hb(ctx);
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        self.rabbit.do_send(DelSocket{ session_id: self.id });
    }
}

/// Handler for `ws::Message`
impl StreamHandler<ws::Message, ws::ProtocolError> for MyWebSocket {
    fn handle(&mut self, msg: ws::Message, ctx: &mut Self::Context) {
        // process websocket messages
        match msg {
            ws::Message::Ping(msg) => {
                self.hb = Instant::now();
                ctx.pong(&msg);
            }
            ws::Message::Pong(_) => {
                self.hb = Instant::now();
            }
            ws::Message::Text(text) => ctx.text(text),
            ws::Message::Binary(bin) => ctx.binary(bin),
            ws::Message::Close(_) => {
                ctx.stop();
            }
            ws::Message::Nop => (),
        }
    }
}

impl MyWebSocket {    
    pub fn new(addr: web::Data<Rc<Addr<RabbitReceiver>>>) -> Self {
        Self { hb: Instant::now(), rabbit: addr, id: Uuid::new_v4() }
    }

    /// helper method that sends ping to client every second.
    ///
    /// also this method checks heartbeats from client
    fn hb(&self, ctx: &mut <Self as Actor>::Context) {
        ctx.run_interval(HEARTBEAT_INTERVAL, |act, ctx| {
            // check client heartbeats
            if Instant::now().duration_since(act.hb) > CLIENT_TIMEOUT {
                // heartbeat timed out
                warn!("Websocket Client heartbeat failed, disconnecting!");
                // stop actor
                ctx.stop();
                // don't try to send a ping
                return;
            }

            ctx.ping("");
        });
    }
}
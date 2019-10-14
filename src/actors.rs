use uuid::Uuid;
use std::sync::{Arc, mpsc};
use std::thread;
use std::rc::Rc;
use std::time::{Duration, Instant};

use actix::prelude::*;
use actix_web::web;
use actix_web_actors::ws;

use lapin::{
  Channel, Connection, ConnectionProperties, 
  message::DeliveryResult,
  types::FieldTable,
  ExchangeKind, options::*,
  Queue,
};

/// How often heartbeat pings are sent
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);
/// How long before lack of client response causes a timeout
const CLIENT_TIMEOUT: Duration = Duration::from_secs(10);

pub struct RabbitReceiver {
    count: usize,
}

impl RabbitReceiver {
    pub fn new(amqp: String, timeout: u64, exchange: &str, queue: &str) -> Result<RabbitReceiver, std::io::Error> {
        let conn = RabbitReceiver::get_connection(amqp, timeout)?;
        let chan = Arc::new(RabbitReceiver::get_channel(conn)?);
        let _ = RabbitReceiver::create_exchange(chan.clone(), exchange)?;
        let q = RabbitReceiver::create_queue(chan.clone(), queue)?;
        RabbitReceiver::create_bindings(chan.clone(), queue, exchange)?;
        RabbitReceiver::consume(chan.clone(), &q)?;

        return Ok(RabbitReceiver{ count: 0 });
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
#[derive(Clone, Message)]
pub struct AddSocket {
    sender: Addr<MyWebSocket>,
}

/// Message sent to Rabbit Receiver to deregister a websocket on disconnection.
#[derive(Clone, Message)]
pub struct DelSocket{}

/// Actor implementing the websocket connection
pub struct MyWebSocket {
    /// Client must send ping at least once per 10 seconds (CLIENT_TIMEOUT),
    hb: Instant,
    rabbit: web::Data<Rc<Addr<RabbitReceiver>>>,
}


impl Actor for MyWebSocket {
    type Context = ws::WebsocketContext<Self>;

    /// Method is called on actor start. We start the heartbeat process here.
    fn started(&mut self, ctx: &mut Self::Context) {
        let addr = ctx.address();
        self.rabbit.do_send(AddSocket{sender: addr});

        self.hb(ctx);
    }
}

/// Handler for `ws::Message`
impl StreamHandler<ws::Message, ws::ProtocolError> for MyWebSocket {
    fn handle(&mut self, msg: ws::Message, ctx: &mut Self::Context) {
        // process websocket messages
        println!("WS: {:?}", msg);
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

    fn started(&mut self, _ctx: &mut Self::Context) {
        println!("websocket is started");
    }
}

impl Drop for MyWebSocket {
        fn drop(&mut self) {
        println!("Dropping a websocket");
        self.rabbit.do_send(DelSocket{});
    }
}

impl MyWebSocket {    
    pub fn new(addr: web::Data<Rc<Addr<RabbitReceiver>>>) -> Self {
        Self { hb: Instant::now(), rabbit: addr }
    }

    /// helper method that sends ping to client every second.
    ///
    /// also this method checks heartbeats from client
    fn hb(&self, ctx: &mut <Self as Actor>::Context) {
        ctx.run_interval(HEARTBEAT_INTERVAL, |act, ctx| {
            // check client heartbeats
            if Instant::now().duration_since(act.hb) > CLIENT_TIMEOUT {
                // heartbeat timed out
                println!("Websocket Client heartbeat failed, disconnecting!");

                // stop actor
                ctx.stop();

                // don't try to send a ping
                return;
            }

            ctx.ping("");
        });
    }
}
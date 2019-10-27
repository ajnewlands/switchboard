use uuid::Uuid;
use std::sync::{Arc, RwLock, mpsc};
use std::thread;
use std::rc::Rc;
use std::time::{Duration, Instant};
use std::collections::HashMap;
use log::{debug, info, warn, error};

use actix::prelude::*;
use actix_web::web;
use actix_web_actors::ws;

use lapin::{
  Channel, Connection, ConnectionProperties, 
  message::DeliveryResult,
  types::{FieldTable, ShortString},
  ExchangeKind, options::*,
  Queue, BasicProperties,
};

extern crate flatbuffers;
#[allow(unused_imports)]
mod messages_generated;
use messages_generated::switchboard::*;

mod util;
use util::string_to_header;

/// How often heartbeat pings are sent
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);
/// How long before lack of client response causes a timeout
const CLIENT_TIMEOUT: Duration = Duration::from_secs(10);

pub struct RabbitReceiver {
    sessions: Arc<RwLock<HashMap<String, Addr<MyWebSocket>>>>,
    rchan: Arc<Channel>,
    wchan: Arc<Channel>,
    id: String,
    ex: String,
    q: Queue,
}

impl RabbitReceiver {
    pub fn new(amqp: String, timeout: u64, exchange: &str, queue: &str) -> Result<RabbitReceiver, std::io::Error> {
        let rconn = RabbitReceiver::get_connection(amqp.clone(), timeout)?;
        let wconn = RabbitReceiver::get_connection(amqp, timeout)?;
        let rchan = Arc::new(RabbitReceiver::get_channel(&rconn)?);
        let wchan = Arc::new(RabbitReceiver::get_channel(&wconn)?);
        let _ = RabbitReceiver::create_exchange(rchan.clone(), exchange)?;
        let q = RabbitReceiver::create_queue(rchan.clone(), queue)?;
        let id =  Uuid::new_v4().to_string();

        info!("Service registered with id {}", id);

        RabbitReceiver::create_bindings(id.clone(), rchan.clone(), queue, exchange)?;

        return Ok(RabbitReceiver{ 
            sessions: Arc::new(RwLock::new(HashMap::with_capacity(64))),
            rchan: rchan.clone(),
            wchan: wchan.clone(),
            id: id,
            ex: String::from(exchange),
            q: q,
            });
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
    fn get_channel(conn: &Connection) -> Result<Channel, std::io::Error> {
        return match conn.create_channel().wait() {
            Ok(ch) => match ch.basic_qos( 30, BasicQosOptions::default()).wait() {
                    Ok(_) => Ok(ch),
                    Err(e) => Err(std::io::Error::new(std::io::ErrorKind::NotConnected, e)), 
            },
            Err(e) => Err(std::io::Error::new(std::io::ErrorKind::NotConnected, e)),
        };
    }

    /// Declare the named exchange, creating it if it doesn exist already.
    fn create_exchange(chan: Arc<Channel>, exchange: &str) -> Result<&str, std::io::Error> {
        let opts = ExchangeDeclareOptions{ passive:false, durable: false, auto_delete: true, internal:false, nowait:false };
        return match chan.exchange_declare(exchange , ExchangeKind::Headers, opts, FieldTable::default()).wait() {
            Ok(_) => Ok(exchange),
            Err(e) => Err(std::io::Error::new(std::io::ErrorKind::NotConnected, e)),
        };
    }

    /// Declare the named queue (creating it if it doesn't exist)
    fn create_queue(chan: Arc<Channel>, queue: &str) -> Result<Queue, std::io::Error> {
        let opts = QueueDeclareOptions{ passive:false, durable:false, exclusive:false, auto_delete:true, nowait:false};
        return match chan.queue_declare(queue, opts, FieldTable::default()).wait() {
            Ok(q) => Ok(q),
            Err(e) => Err(std::io::Error::new(std::io::ErrorKind::NotConnected, e)),
        };
    }

    fn create_bindings(id: String, chan: Arc<Channel>, queue: &str, exchange: &str) -> Result<(), std::io::Error> {
        let mut fields = FieldTable::default();
        fields.insert(ShortString::from("dest_id"), string_to_header(&id));
        fields.insert(ShortString::from("type"), string_to_header("ViewUpdate"));

        return match chan.queue_bind(queue, exchange, "", QueueBindOptions::default(), fields).wait() {
            Ok(_) => Ok(()),
            Err(e) => Err(std::io::Error::new(std::io::ErrorKind::NotConnected, e)),
        };
    }

    fn consume(sessions: Arc<RwLock<HashMap<String, Addr<MyWebSocket>>>>, chan: Arc<Channel>, queue: &Queue) -> Result<(), std::io::Error> {
        let opts = BasicConsumeOptions{ no_local: true, no_ack: false, exclusive: false, nowait: false };
        return match chan.basic_consume(queue, "", opts, FieldTable::default()).wait() {
            Ok(con) => Ok(con.set_delegate(Box::new(move | delivery: DeliveryResult |{ 
                match delivery {
                    Ok(Some(delivery)) => {
                        // TODO error checking here
                        let headers = delivery.properties.headers().as_ref().unwrap().inner();
                        let session = match &headers["session"] {
                            lapin::types::AMQPValue::LongString(s) => s.as_str(),
                            _ => "",
                        };
                        // TODO decompose this
                        // TODO use panic::catch_unwind to avoid exploding when buffer is not a
                        // flatbuffer
                        let msg = get_root_as_msg(&delivery.data); 
                        debug!("Got rabbit message, type is {:?}", msg.content_type());
                        match sessions.read().unwrap().get(session) {
                            Some(address) => address.do_send(FlatbuffMessage{ flatbuffer: bytes::Bytes::from(delivery.data), session: String::new() }), 
                            None => warn!("Received message for non-existent session {} in {:?}", session, sessions.read().unwrap().keys()),
                        };
                        chan.basic_ack(delivery.delivery_tag, BasicAckOptions::default()).wait().expect("ACK failed")
                    }, // Got message
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
        self.sessions.write().unwrap().insert(msg.session_id.to_string(), msg.sender);
        debug!("Connected sockets now {}, added session {}", self.sessions.read().unwrap().len(), msg.session_id);
    }
}

/// when a socket is closed, cease tracking it's session id and address.
impl Handler<DelSocket> for RabbitReceiver {
    type Result = ();

    fn handle (&mut self, msg: DelSocket, _ctx: &mut Context<Self>)  {
        let mut locked = self.sessions.write().unwrap();
        match locked.remove(&(msg.session_id.to_string())) {
            Some(_) => debug!("Removed session {}, remaining sessions {}", msg.session_id, locked.len()),
            None => warn!("Got disconnect for non-existent session {}", msg.session_id),
        };
    }
}

impl Handler<FlatbuffMessage> for RabbitReceiver {
    type Result = ();

    fn handle(&mut self, msg: FlatbuffMessage, _ctx: &mut Context<Self>) {
        let mut headers = FieldTable::default();
        let root = get_root_as_msg(&msg.flatbuffer); 
        headers.insert(ShortString::from("sender_id"), string_to_header(&self.id));
        headers.insert(ShortString::from("session"), string_to_header(&msg.session));
        headers.insert(ShortString::from("type"), string_to_header(& enum_name_content(root.content_type())));
        
        let props = BasicProperties::default().with_headers(headers);

        let payload = msg.flatbuffer;
        match self.wchan.basic_publish(&self.ex, "", BasicPublishOptions::default(), payload.to_vec(), props).wait() {
            Ok(_) => debug!("sent msg to bus"),
            Err(e) => error!("failed dispatch to bus: {}", e),
        };
    }
}

impl Handler<EchoRequest> for RabbitReceiver {
    type Result = ();

    fn handle(&mut self, msg: EchoRequest, _ctx: &mut Context<Self>) {
        let mut headers = FieldTable::default();
        headers.insert(ShortString::from("sender_id"), string_to_header(&self.id));
        
        let props = BasicProperties::default().with_headers(headers);

        let payload = msg.content.into_bytes();
        match self.wchan.basic_publish(&self.ex, "", BasicPublishOptions::default(), payload, props).wait() {
            Ok(_) => debug!("sent msg to bus"),
            Err(e) => error!("failed dispatch to bus: {}", e),
        };
    }
}

// Rabbit receiver object
impl Actor for RabbitReceiver {
    type Context = Context<Self>;

    fn started(&mut self, _ctx: &mut Self::Context) {
        match RabbitReceiver::consume(self.sessions.clone(), self.rchan.clone(), &self.q) {
            Ok(_) => info!("Starting Rabbit consumption"),
            Err(e) => panic!("Unable to consume from rabbit: {}", e),
        };
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        match self.wchan.close(503, "Service shut down").wait() {
            Ok(_) => (),
            Err(e) => warn!("Attempted to close rabbit channel but got: {}", e),
        };
        match self.rchan.close(503, "Service shut down").wait() {
            Ok(_) => (),
            Err(e) => warn!("Attempted to close rabbit channel but got: {}", e),
        };
        debug!("Stopped rabbit receiver");
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

/// Simple message to trigger a dumb rabbit message
#[derive(Clone, Message)]
pub struct EchoRequest {
    content: String,
}

/// Hand off flatbuffer message between actors
#[derive(Clone, Message)]
pub struct FlatbuffMessage {
    flatbuffer: bytes::Bytes,
    session: String,
}


/// Actor implementing the websocket connection
pub struct MyWebSocket {
    /// Client must send ping at least once per 10 seconds (CLIENT_TIMEOUT),
    hb: Instant,
    rabbit: web::Data<Rc<Addr<RabbitReceiver>>>,
    id: Uuid,
}

impl MyWebSocket {
    fn process_buffer(& self, data: bytes::Bytes) {
        let msg = get_root_as_msg(&data); 
        debug!("Websocket received message, type is {:?}", msg.content_type());
        self.rabbit.do_send(FlatbuffMessage{ flatbuffer: data, session: self.id.to_string() } );
    }
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
            ws::Message::Text(text) => self.rabbit.do_send(EchoRequest{content: text}),
            ws::Message::Binary(bin) => self.process_buffer(bin), 
            ws::Message::Close(_) => {
                ctx.stop();
            }
            ws::Message::Nop => (),
        }
    }
}


impl Handler<FlatbuffMessage> for MyWebSocket {
    type Result = ();

    fn handle(&mut self, msg: FlatbuffMessage, ctx: &mut Self::Context) {
        let root = get_root_as_msg(&msg.flatbuffer); 
        debug!("Message passed from Rabbit->Websocket, type is {:?}, size is {}", root.content_type(), msg.flatbuffer.len());
        ctx.binary(msg.flatbuffer);
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

use uuid::Uuid;
use std::sync::{Arc, RwLock};
use std::thread;
use std::panic;
use std::rc::Rc;
use std::cell::RefCell;
use std::time::{Duration, Instant};
use std::collections::HashMap;
use log::{debug, info, warn, error};

use actix::prelude::*;
use actix_web::web;
use actix_web_actors::ws;

use crossbeam_channel::{unbounded, Receiver, Sender, select};

use amiquip::{Connection, Publish, ConsumerMessage, ConsumerOptions, QueueDeclareOptions, ExchangeType, ExchangeDeclareOptions, FieldTable, AmqpValue, AmqpProperties, Channel };

extern crate flatbuffers;
use flatbuffers::FlatBufferBuilder;
#[allow(unused_imports)]
mod messages_generated;
use messages_generated::switchboard::*;

/// How often heartbeat pings are sent
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);
/// How long before lack of client response causes a timeout
const CLIENT_TIMEOUT: Duration = Duration::from_secs(10);

/// AMIQUIP based rabbit handler
struct Rabbit {
    chan: Rc<Channel>,
    _conn: Rc<RefCell<Connection>>,
    rx: Receiver<FlatbuffMessage>,
    ex: String,
    id: String,
}
unsafe impl Send for Rabbit{}

impl Rabbit {
    pub fn new( amqp: &str, exchange: &str, queue: &str, rx: Receiver<FlatbuffMessage>) -> Result<Rabbit, String> {
        let conn = Rc::new( 
            RefCell::new(
                Connection::insecure_open(amqp)
                    .map_err(|e| format!("Failed to open rabbit connection: {:?}", e))?
                )
            );

        let chan = Rc::new(
            conn.borrow_mut().open_channel(None)
                .map_err(|e| format!("Failed to open rabbit channel: {:?}", e))?
            );


        let id = Uuid::new_v4().to_string();
        let rabbit = Rabbit { chan, _conn: conn, rx, ex: String::from(exchange), id };

        rabbit.declare_exchange(exchange)?;
        rabbit.bind(queue, exchange)?;

        return Ok(rabbit);
    }

    fn declare_exchange(&self, exchange: &str) -> Result<(), String> {
        let opts = ExchangeDeclareOptions{ durable: false, auto_delete: true, internal:false, arguments: FieldTable::default() };
        self.chan.exchange_declare(ExchangeType::Headers, exchange, opts)
            .map_err(|e| format!("Failed to create exchange {}: {:?}", exchange, e))?;

        Ok(())
    }

    fn bind(&self, queue: &str, exchange: &str) -> Result<(), String> {
        let mut fields = FieldTable::default();
        fields.insert( String::from("type"), AmqpValue::LongString(String::from("ViewUpdate")));
        fields.insert( String::from("dest_id"), AmqpValue::LongString(self.id.clone()));

        let opts = QueueDeclareOptions{ durable: false, exclusive: false, auto_delete: true, arguments: FieldTable::default() };
        self.chan.queue_declare(queue,  opts)
            .map_err(|e| format!("Failed to create queue {}: {:?}", queue, e))?;

        self.chan.queue_bind( queue, exchange, "", fields )
            .map_err(|e| format!("Failed to bind queue {}: {:?}", queue, e))?;
        info!("Service registered with id {}", self.id);
        Ok(())
    }

    fn consume(&self, queue: &str, addr: Arc<Addr<RabbitReceiver>>) -> Result<(), String> {
        let opts = QueueDeclareOptions{ durable: false, exclusive: false, auto_delete: true, arguments: FieldTable::default() };
        let q = self.chan.queue_declare(queue,  opts)
            .map_err(|e| format!("Failed to create queue {}: {:?}", queue, e))?;

        let rabbit = q.consume(ConsumerOptions::default())
            .map_err(|e| format!("Could not start rabbit consumer: {:?}", e))?;

        info!("Starting consuming rabbit messages");

        
        loop { // obviously this won't shut down gracefully..
            select! {
                recv(*rabbit.receiver()) -> r => match r {
                    Ok(ConsumerMessage::Delivery(msg)) => {
                        match msg.properties.headers().as_ref(){
                            None => warn!("Disregarding message without headers"),
                            Some(headers) => {
                                if let AmqpValue::LongString(session) = &headers["session"] {
                                    debug!("Got a message for session {}", session);
                                    match panic::catch_unwind(|| get_root_as_msg(&msg.body)) {
                                        Ok(root) => { 
                                            debug!("Got rabbit message, type is {:?}, at {}", root.content_type(), (std::time::SystemTime::now().duration_since(std::time::SystemTime::UNIX_EPOCH).unwrap().as_millis()));
                                            addr.do_send(WebsocketMessage{ flatbuffer: bytes::Bytes::from(msg.body), session: session.to_string() });
                                        },
                                        Err(_) => error!("Dropping an invalid message buffer"),
                                    };
                                } else {
                                    warn!("disregarding message without a session header");
                                }
                            },
                        };
                    },
                    e => {
                        error!("Rabbit connection closed, stopping event loop: {:?}", e);
                        break;
                    }
                },
                recv(self.rx) -> r => match r { // either got a message from the websocket side or channel closure
                    Ok(msg) => {
                        let mut headers = FieldTable::default();
                        let root = get_root_as_msg(&msg.flatbuffer); 
                        headers.insert(String::from("sender_id"), AmqpValue::LongString( self.id.clone() ));
                        headers.insert(String::from("session"), AmqpValue::LongString(msg.session));
                        headers.insert(String::from("type"), AmqpValue::LongString(enum_name_content(root.content_type()).to_string()));
                 
                        let props = AmqpProperties::default().with_headers(headers);
                        let opts = Publish{ body: &msg.flatbuffer, routing_key: String::from(""), mandatory: false, immediate: false, properties: props };
                        self.chan.basic_publish( self.ex.clone(), opts );
                
                        debug!("Wrote {:?} to bus", root.content_type());
                    },
                    Err(e) => {
                        error!("Websocket channel closed, stopping event loop: {:?}", e);
                        break;
                    },
                },
            }
        }

        Ok(())
    }
}

/// END AMIQUIP rabbit handler

pub struct RabbitReceiver {
    sessions: Arc<RwLock<HashMap<String, Addr<MyWebSocket>>>>,
    rabbit: Option<thread::JoinHandle<()>>,
    tx: Option<Sender<FlatbuffMessage>>,
}

impl RabbitReceiver {
    pub fn new(amqp: String, exchange: String, queue: String) -> Result<RabbitReceiver, String> {

        return Ok(RabbitReceiver{ 
            sessions: Arc::new(RwLock::new(HashMap::with_capacity(64))),
            rabbit: None,
            tx: None,
            });
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
            Some(_) => { 
                debug!("Removed session {}, remaining sessions {}", msg.session_id, locked.len());
                let mut builder = FlatBufferBuilder::new();
                let end = ViewEnd::create(&mut builder, &ViewEndArgs{});
                let ses = builder.create_string(&msg.session_id.to_string());
                let buffer = Msg::create(&mut builder, &MsgArgs{
                    content_type: Content::ViewEnd,
                    session: Some(ses),
                    content: Some(end.as_union_value()),
                });
                builder.finish(buffer, None);
                if let Some(tx) = &self.tx {
                    tx.send(FlatbuffMessage{flatbuffer: bytes::Bytes::from(builder.finished_data()), session: msg.session_id.to_string()});
                }
            },
            None => warn!("Got disconnect for non-existent session {}", msg.session_id),
        };
    }
}

impl Handler<FlatbuffMessage> for RabbitReceiver {
    type Result = ();

    fn handle(&mut self, msg: FlatbuffMessage, _ctx: &mut Context<Self>) {
        if let Some(tx) = &self.tx {
            tx.send(msg);
        }
    }
}

impl Handler<WebsocketMessage> for RabbitReceiver {
    type Result = ();

    fn handle(&mut self, msg: WebsocketMessage, _ctx: &mut Context<Self>) {
        match self.sessions.read().unwrap().get(&msg.session) {
            Some(address) => {
                address.do_send(FlatbuffMessage{ flatbuffer: msg.flatbuffer, session: msg.session });
            },
            None => {
                warn!("Received message for non-existent session {}", msg.session);
            },
        };
    }
}

// Rabbit receiver object
impl Actor for RabbitReceiver {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        let (tx, rx) = unbounded();
        self.tx = Some(tx);
        let addr = Arc::new(ctx.address().clone());

        let t = thread::spawn(move|| {
            Rabbit::new("amqp://localhost", "switchboard", "switchboard", rx).and_then(|r|r.consume("switchboard", addr.clone()));
        });
        self.rabbit = Some(t);
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
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

/// Hand off flatbuffer message between actors -> to rabbit
#[derive(Clone, Message)]
pub struct FlatbuffMessage {
    flatbuffer: bytes::Bytes,
    session: String,
}

/// Hand off flatbuffer message between actors -> to browser
#[derive(Clone, Message)]
pub struct WebsocketMessage {
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
    /// TODO unwind on failure and log
    fn process_buffer(& self, data: bytes::Bytes) {
        let msg = get_root_as_msg(&data); 
        debug!("Websocket received message, type is {:?}, at {}", msg.content_type(), (std::time::SystemTime::now().duration_since(std::time::SystemTime::UNIX_EPOCH).unwrap().as_millis()));
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
            ws::Message::Text(text) => (),
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

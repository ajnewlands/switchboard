//! websocket <-> message queue switchboard
//! Open `http://localhost:8080/ws/index.html` in browser to test

use std::time::{Duration, Instant};
use std::sync::{Mutex, Arc};

use actix::prelude::*;
use actix_files as fs;
use actix_web::{middleware, web, App, Error, HttpRequest, HttpResponse, HttpServer};
use actix_web_actors::ws;

mod rabbit;

/// How often heartbeat pings are sent
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);
/// How long before lack of client response causes a timeout
const CLIENT_TIMEOUT: Duration = Duration::from_secs(10);



/// do websocket handshake and start `MyWebSocket` actor
fn ws_index(r: HttpRequest, stream: web::Payload, data: web::Data<Arc<Mutex<Addr<rabbit::RabbitReceiver>>>>) -> Result<HttpResponse, Error> {
    data.lock().unwrap().do_send(rabbit::Add{});

    return ws::start(MyWebSocket::new(data.clone()), &r, stream);
}

/// Actor implementing the websocket connection
struct MyWebSocket {
    /// Client must send ping at least once per 10 seconds (CLIENT_TIMEOUT),
    hb: Instant,
    rabbit: web::Data<Arc<Mutex<Addr<rabbit::RabbitReceiver>>>>,
}


impl Actor for MyWebSocket {
    type Context = ws::WebsocketContext<Self>;

    /// Method is called on actor start. We start the heartbeat process here.
    fn started(&mut self, ctx: &mut Self::Context) {
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
        self.rabbit.lock().unwrap().do_send(rabbit::Goodbye{});
    }
}

impl MyWebSocket {    
    fn new(addr: web::Data<Arc<Mutex<Addr<rabbit::RabbitReceiver>>>>) -> Self {
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

fn main() -> std::io::Result<()> {
    std::env::set_var("RUST_LOG", "actix_server=info,actix_web=info");
    env_logger::init();

    let sys = System::new("switchboard");
    let rabbit = rabbit::RabbitReceiver::default().start();

    let r = HttpServer::new(move || {
        App::new()
            .data(Arc::new(Mutex::new(rabbit.clone())))
            // enable logger
            .wrap(middleware::Logger::default())
            // websocket route
            .service(web::resource("/ws/").route(web::get().to(ws_index)))
            // static files
            .service(fs::Files::new("/", "static/").index_file("index.html"))
    })
    // start http server on 127.0.0.1:8080
    .bind("127.0.0.1:8080")?
    .run();

    return match r {
        Err(x) => Err(x),
        _ => sys.run()
    };

}
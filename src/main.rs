//! websocket <-> message queue switchboard
//! Open `http://localhost:8080/ws/index.html` in browser to test
use std::rc::Rc;
use log::info;

use actix::prelude::*;
use actix_files as fs;
use actix_web::{web, App, Error, HttpRequest, HttpResponse, HttpServer};
use actix_web_actors::ws;

mod actors;


/// do websocket handshake and start `MyWebSocket` actor
fn ws_index(r: HttpRequest, stream: web::Payload, data: web::Data<Rc<Addr<actors::RabbitReceiver>>>) -> Result<HttpResponse, Error> {
    return ws::start(actors::MyWebSocket::new(data.clone()), &r, stream);
}

fn main() -> std::io::Result<()> {
    std::env::set_var("RUST_LOG", "debug,lapin=info,tokio_reactor=info,actix_server=info,actix_web=info");
    env_logger::init();
    let _ = System::new("switchboard");
    
    let amqp = std::env::var("AMQP_ADDR").unwrap_or_else(|_| "amqp://127.0.0.1:5672/%2f".into());
    let timeout: u64 = 30000;

    info!("Connecting to Rabbit..");
    let rabbit = actors::RabbitReceiver::new(amqp, timeout, "switchboard", "switchboard")?.start();
    info!("Connected to Rabbit");

    HttpServer::new(move || {
        App::new()
            .data(Rc::new(rabbit.clone()))
            //.wrap(middleware::Logger::default()) // log the queries on the way through (spammy)
            .service(web::resource("/ws/").route(web::get().to(ws_index)))
            .service(fs::Files::new("/", "static/").index_file("index.html"))
    })
    // start http server on 127.0.0.1:8080
    .bind("127.0.0.1:8080")?
    .run()?;

    Ok(())
}

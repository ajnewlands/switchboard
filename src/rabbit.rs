use actix::prelude::*;
use uuid::Uuid;


pub struct RabbitReceiver {
    count: usize,
}

impl Default for RabbitReceiver {
    fn default() -> RabbitReceiver {
        return RabbitReceiver{count:0};
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
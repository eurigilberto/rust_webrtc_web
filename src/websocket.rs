use crate::{clone_move, log_fmt, utils::*};
use wasm_bindgen::prelude::*;
use web_sys::{js_sys::*, *};

#[derive(Clone)]
pub struct MessageGatherer {
    should_exit: SmartCell<bool>,
    messages: SmartPtr<Vec<(String, String)>>,
}
impl MessageGatherer {
    pub fn new() -> Self {
        Self {
            should_exit: SmartCell::new(false),
            messages: SmartPtr::new(Vec::new()),
        }
    }
    pub fn drain(&self) -> Vec<(String, String)> {
        self.messages.borrow_mut().drain(..).collect()
    }
    pub fn stop(&self) {
        self.should_exit.set(true);
    }
}
impl SocketListener for MessageGatherer {
    fn listen(&self, handler: &WebSocketHandler, command: &str, data: &str) -> bool {
        self.messages
            .borrow_mut()
            .push((command.into(), data.into()));
        !self.should_exit.get()
    }
}

pub trait SocketListener {
    fn listen(&self, handler: &WebSocketHandler, command: &str, data: &str) -> bool;
}

pub struct WebSocketHandler {
    pub id: SmartCell<u64>,
    pub socket: WebSocket,
    listeners: SmartPtr<Vec<Box<dyn SocketListener>>>,
    queued_listeners: SmartPtr<Vec<Box<dyn SocketListener>>>,
}

impl Clone for WebSocketHandler {
    fn clone(&self) -> Self {
        Self {
            id: self.id.clone(),
            socket: self.socket.clone(),
            listeners: self.listeners.clone(),
            queued_listeners: self.queued_listeners.clone(),
        }
    }
}

impl WebSocketHandler {
    pub fn new(id: u64, socket: WebSocket) -> Self {
        Self {
            id: SmartCell::new(id),
            socket,
            listeners: SmartPtr::new(Vec::new()),
            queued_listeners: SmartPtr::new(Vec::new()),
        }
    }
    pub fn drain_queue(&self) {
        let mut listeners = self.listeners.borrow_mut();
        for listener in self.queued_listeners.borrow_mut().drain(..) {
            listeners.push(listener);
        }
    }
    pub fn on_received(&self, command: &str, data: &str) {
        self.drain_queue();
        let mut listeners = self.listeners.borrow_mut();
        for idx in (0..listeners.len()).rev() {
            if !listeners[idx].listen(self, command, data) {
                listeners.remove(idx);
            }
        }
    }
    pub fn push_listener(&self, listener: impl SocketListener + 'static) {
        self.queued_listeners.borrow_mut().push(Box::new(listener));
    }
}

pub struct WebSocketCreationConfig<'a> {
    url: &'a str,
    retry_count: u32,
    delay_per_retry: u32,
    timeout: u32,
}

pub async fn start_websocket<'url>(
    config: WebSocketCreationConfig<'_>,
) -> Result<WebSocketHandler, ()> {
    match try_start_websocket(config).await {
        Ok((id, socket)) => {
            let handler = WebSocketHandler::new(id, socket.clone());
            let on_message_received = clone_move!(handler => move |e: MessageEvent| {
                let msg = e.data().as_string().unwrap();
                log_fmt!("Received msg {}", msg);
                let command_range = msg.find(' ').unwrap_or(msg.len());
                let command = &msg[..command_range];
                let msg = msg[command_range..].trim();
                handler.on_received(command, msg);
            });
            let on_message_received: Closure<dyn FnMut(_)> = Closure::new(on_message_received);
            socket.set_onmessage(Some(on_message_received.as_ref().unchecked_ref()));
            on_message_received.forget();
            return Ok(handler);
        }
        Err(_) => Err(()),
    }
}

async fn try_start_websocket(config: WebSocketCreationConfig<'_>) -> Result<(u64, WebSocket), ()> {
    let mut creation_try_count = 0;
    loop {
        web_sleep(config.delay_per_retry as i32).await;
        let Ok(websocket) = WebSocket::new(config.url) else {
            log_fmt!("Failed connection object");
            continue;
        };
        let socket_error = SmartCell::new(false);
        let on_error = Closure::<dyn FnMut()>::new(clone_move!(socket_error => move || {
            socket_error.set(true);
        }));
        let socket_id = NullSmartPtr::<u64>::null();
        let on_message = Closure::<dyn FnMut(_)>::new(
            clone_move!(websocket, socket_id, socket_error => move |e: MessageEvent| {
                let msg = e.data().as_string().unwrap();
                let command_range = msg.find(' ').unwrap_or(msg.len());
                let command = &msg[..command_range];
                if command == "#ID" {
                    let msg = msg[command_range..].trim();
                    let id_range = msg.find(' ').unwrap_or(msg.len());
                    let maybe_id = u64::from_str_radix(&msg[..id_range], 10);
                    if maybe_id.is_err() {
                        socket_error.set(true);
                        let _ = websocket.close();
                        return;
                    }else {
                        socket_id.set(maybe_id.unwrap());
                    }
                }
            }),
        );
        websocket.set_onerror(Some(on_error.as_ref().unchecked_ref()));
        websocket.set_onmessage(Some(on_message.as_ref().unchecked_ref()));

        // Wait for websocket creation or failure
        let wait_creation = move || {
            if socket_error.get() {
                return Err(());
            }
            let Some(id) = socket_id.borrow() else {
                return Ok(None);
            };
            websocket.set_onerror(None);
            websocket.set_onmessage(None);
            log_fmt!("Creation try count {}", creation_try_count);
            return Ok(Some((*id, websocket.clone())));
        };
        if let Ok((id, socket)) = wait_until(100, config.timeout, wait_creation).await {
            return Ok((id, socket));
        }

        creation_try_count += 1;
        if creation_try_count >= config.retry_count {
            return Err(());
        }
    }
}

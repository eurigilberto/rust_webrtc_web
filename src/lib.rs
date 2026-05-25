use std::{
    cell::{Cell, RefCell},
    ops::Deref,
    rc::Rc,
};

use wasm_bindgen::prelude::*;
use web_sys::{
    Event, MessageEvent, RtcConfiguration, RtcDataChannel, RtcDataChannelEvent, RtcIceGatheringState, RtcPeerConnection, RtcPeerConnectionIceEvent, RtcSessionDescriptionInit, WebSocket, js_sys::{self, ArrayBuffer, Uint8Array}
};

use wasm_bindgen_futures::JsFuture;
use web_sys::window;

#[macro_export]
macro_rules! clone_move {
    ($($n:ident),+ => $closure:expr) => {
        {
            $( let $n = $n.clone(); )+
            $closure
        }
    };
}

pub async fn sleep(ms: i32) -> Result<(), JsValue> {
    let promise = js_sys::Promise::new(&mut |resolve, _| {
        window()
            .unwrap()
            .set_timeout_with_callback_and_timeout_and_arguments_0(&resolve, ms)
            .expect("should register setTimeout");
    });

    JsFuture::from(promise).await?;
    Ok(())
}

#[derive(Debug, Clone)]
pub struct SmartPtr<T>(Rc<RefCell<Option<T>>>);
impl<T> SmartPtr<T> {
    pub fn new(t: T) -> Self {
        Self(Rc::new(RefCell::new(Some(t))))
    }
    pub fn null() -> Self {
        Self(Rc::new(RefCell::new(None)))
    }
    pub fn borrow(&self) -> std::cell::Ref<'_, Option<T>> {
        self.0.borrow()
    }
    pub fn borrow_mut(&self) -> std::cell::RefMut<'_, Option<T>> {
        self.0.borrow_mut()
    }
}

#[derive(Clone)]
pub struct WebSocketConn {
    conn_id: Rc<Cell<Option<u64>>>,
    socket: WebSocket,
}
impl WebSocketConn {
    pub fn new(socket: WebSocket) -> Self {
        Self {
            conn_id: Rc::new(Cell::new(None)),
            socket,
        }
    }
    pub fn get_id(&self) -> u64 {
        self.conn_id.get().unwrap()
    }
}
impl Deref for WebSocketConn {
    type Target = WebSocket;
    fn deref(&self) -> &Self::Target {
        &self.socket
    }
}

#[macro_export]
macro_rules! log_fmt {
    ($($arg:tt)*) => {
        {
            let msg = format!($($arg)*);
            web_sys::console::log_1(&JsValue::from_str(&msg));
            msg
        }
    };
}

pub fn start_websocket(
    url: &str,
    mut on_ready: Box<dyn FnMut(WebSocketConn)>,
    mut on_received: Box<dyn FnMut(WebSocketConn, String, String)>,
) -> Result<WebSocketConn, ()> {
    let Ok(websocket) = WebSocket::new(url) else {
        return Err(());
    };
    websocket.set_binary_type(web_sys::BinaryType::Arraybuffer);
    let connection = WebSocketConn::new(websocket);
    let cloned_conn = connection.clone();

    let on_message_received = Closure::<dyn FnMut(_)>::new(move |e: MessageEvent| {
        let message = e.data().as_string().unwrap();
        let mut split = message.split(' ');
        let command = split.next();
        if command == Some("#ID") {
            let id = split.next().unwrap();
            let id = u64::from_str_radix(id, 10).unwrap();
            cloned_conn.conn_id.set(Some(id));
            log_fmt!("ID Received {}", id);
            (on_ready)(cloned_conn.clone());
        } else if let Some(command) = command {
            let data: String = split.collect::<Vec<&str>>().join(" ");
            (on_received)(cloned_conn.clone(), command.into(), data);
        } else {
            log_fmt!("Received empty message");
        }
    });
    connection.set_onmessage(Some(on_message_received.as_ref().unchecked_ref()));
    on_message_received.forget();
    //connection.closures.borrow_mut().push(on_message_received);

    let cloned_conn = connection.clone();
    let on_connection_opened = Closure::<dyn FnMut(_)>::new(move |_: MessageEvent| {
        cloned_conn.send_with_str("Test Init Message").unwrap();
    });
    connection.set_onopen(Some(on_connection_opened.as_ref().unchecked_ref()));
    on_connection_opened.forget();
    //connection.closures.borrow_mut().push(on_connection_opened);

    Ok(connection)
}

fn create_rtc_connection() -> RtcPeerConnection {
    let configuration = RtcConfiguration::new();
    let servers = [js_sys::JSON::parse("{\"urls\": \"stun:stun.l.google.com:19302\"}").unwrap()];
    let ice_servers = js_sys::Array::new();
    for server in servers {
        ice_servers.push(&server);
    }
    configuration.set_ice_servers(&ice_servers);
    RtcPeerConnection::new_with_configuration(&configuration)
        .expect("Could not create rtc connection")
}

pub trait DataChannel: Clone {
    fn set_connection(&self, conn: RtcPeerConnection);
    fn channel_created(&self, channel: RtcDataChannel);
    fn offer_created(&self, offer: String);
    fn on_open(&self, event: RtcDataChannelEvent);
    fn on_message(&self, data: Uint8Array);
    fn on_closed(&self);
}

pub async fn start_from_offer(offer: String, data_channel: impl DataChannel + 'static) {
    let rtc_connection = create_rtc_connection();
    data_channel.set_connection(rtc_connection.clone());

    let on_channel_received =
        Closure::<dyn FnMut(_)>::new(clone_move!(data_channel => move |e: RtcDataChannelEvent| {
            let channel = e.channel();
            setup_channel_closures(channel.clone(), data_channel.clone());
        }));
    rtc_connection.set_ondatachannel(Some(on_channel_received.as_ref().unchecked_ref()));
    on_channel_received.forget();

    set_remote_offer(rtc_connection.clone(), offer).await;
    let answer = rtc_connection
        .create_answer()
        .await
        .expect("Could not create answer");
    let answer: RtcSessionDescriptionInit = answer.into();
    rtc_connection
        .set_local_description(&answer)
        .await
        .expect("Could not set local desc");

    setup_offer_creation(rtc_connection, data_channel);
}

pub async fn set_remote_offer(conn: RtcPeerConnection, offer: String) {
    let remote_offer: JsValue = js_sys::JSON::parse(&offer).expect("Could not parse offer");
    let remote_offer: RtcSessionDescriptionInit = remote_offer.into();
    conn.set_remote_description(&remote_offer)
        .await
        .expect("Could not set remote description");
}

pub async fn start_rtc_channel(data_channel: impl DataChannel + 'static) {
    let rtc_connection = create_rtc_connection();
    data_channel.set_connection(rtc_connection.clone());
    let channel = rtc_connection.create_data_channel("Data Channel");
    channel.set_binary_type(web_sys::RtcDataChannelType::Arraybuffer);

    setup_channel_closures(channel.clone(), data_channel.clone());

    let offer: JsValue = rtc_connection
        .create_offer()
        .await
        .expect("Could not create offer");
    let offer: RtcSessionDescriptionInit = offer.into();
    rtc_connection
        .set_local_description(&offer)
        .await
        .expect("Could not set local description");

    setup_offer_creation(rtc_connection, data_channel);
}

fn setup_channel_closures(channel: RtcDataChannel, data_channel: impl DataChannel + 'static) {
    data_channel.channel_created(channel.clone());

    let on_channel_open =
        Closure::<dyn FnMut(_)>::new(clone_move!(data_channel => move |e| data_channel.on_open(e)));
    channel.set_onopen(Some(on_channel_open.as_ref().unchecked_ref()));
    on_channel_open.forget();

    
    let on_channel_message = Closure::<dyn FnMut(MessageEvent)>::new(
        clone_move!(data_channel => move |e: MessageEvent| {
            let buffer: ArrayBuffer = e.data().into();
            let buffer = Uint8Array::new(&buffer);
            log_fmt!("Received: {:?}", buffer);
            data_channel.on_message(buffer);
        }),
    );
    channel.set_onmessage(Some(on_channel_message.as_ref().unchecked_ref()));
    on_channel_message.forget();
}

fn setup_offer_creation(conn: RtcPeerConnection, data_channel: impl DataChannel + 'static) {
    let should_send = SmartPtr::new(false);

    let on_ice_candidate = Closure::<dyn FnMut(Event)>::new(
        clone_move!(conn, data_channel, should_send => move |e: Event|{
            if conn.ice_gathering_state() == RtcIceGatheringState::Complete && !should_send.borrow().expect("No more candidates"){
                *should_send.borrow_mut() = Some(true);
                send_offer(conn.clone(), data_channel.clone());
            }
        }),
    );
    conn.set_onicegatheringstatechange(Some(on_ice_candidate.as_ref().unchecked_ref()));
    on_ice_candidate.forget();

    wasm_bindgen_futures::spawn_local(clone_move!(conn, data_channel, should_send => async move {
        sleep(2500).await.unwrap();
        if !should_send.borrow().expect("Candidates timeout"){
            *should_send.borrow_mut() = Some(true);
            send_offer(conn.clone(), data_channel.clone())
        }
    }));
}

fn send_offer(conn: RtcPeerConnection, data_channel: impl DataChannel + 'static) {
    let offer = conn.local_description();
    if let Some(offer) = offer {
        log_fmt!("All candidates have been gathered");
        let offer = js_sys::JSON::stringify(&offer).unwrap();
        data_channel.offer_created(offer.as_string().unwrap());
    } else {
        log_fmt!("No offer could be created");
    }
}

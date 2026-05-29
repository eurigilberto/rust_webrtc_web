use std::collections::VecDeque;

use crate::{clone_move, log_fmt, utils::*, websocket::*};
use wasm_bindgen::prelude::*;
use web_sys::{js_sys::*, *};

mod inner;
use inner::*;

pub enum RtcConnectionError {
    PeerDoesNotExist,
    IdParsingError,
    RtcConnectionInit,
    RtcConnectionFailed,
    RtcConnectionTimeout,
    RtcChannelNeverReceived,
    OfferCreation,
    OfferSDPCreation,
    AnswerCreation,
    SetLocalDescription,
    IceConnectionFailed,
    SetRemoteDescription,
    WebSocketSend,
    CheckResponseTimeout,
    RequestP2PResponseTimeout,
    WaitAnswerTimeout,
}

#[derive(Debug, Clone, Copy)]
pub struct ConnectionTimeouts {
    pub ice_gathering: u32,
    pub channel_connected: u32,
    pub answer_received: u32,
    pub socket_response: u32,
}

#[derive(Clone)]
pub struct DataChannel {
    channel: RtcDataChannel,
    is_open: SmartCell<bool>,
    copy_target: SmartPtr<Vec<u8>>,
    queue: SmartPtr<VecDeque<u8>>,
    msg_size: SmartPtr<VecDeque<usize>>,
}

impl DataChannel {
    pub fn new(channel: RtcDataChannel) -> Self {
        let is_open = SmartCell::new(true);
        let data_channel = Self {
            channel: channel.clone(),
            is_open: is_open.clone(),
            copy_target: SmartPtr::new(Vec::new()),
            queue: SmartPtr::new(VecDeque::new()),
            msg_size: SmartPtr::new(VecDeque::new()),
        };
        channel.set_onopen(None);
        let on_message: Closure<dyn FnMut(_)> =
            Closure::new(clone_move!(data_channel => move |e: MessageEvent|{
                let buffer: ArrayBuffer = e.data().into();
                let buffer = Uint8Array::new(&buffer);
                data_channel.push_message(buffer);
            }));
        channel.set_onmessage(Some(on_message.as_ref().unchecked_ref()));
        on_message.forget();

        let on_close: Closure<dyn FnMut(_)> = Closure::new(clone_move!(is_open => move |_: Event|{
            is_open.set(false);
        }));
        channel.set_onclose(Some(on_close.as_ref().unchecked_ref()));
        on_close.forget();

        data_channel
    }
    pub fn is_open(&self) -> bool {
        self.is_open.get()
    }
    pub fn pop_message(&self, message: &mut Vec<u8>) -> bool {
        let Some(size) = self.msg_size.borrow_mut().pop_front() else {
            return false;
        };
        if size == 0 {
            return false;
        }
        message.extend(self.queue.borrow_mut().drain(..size));
        true
    }
    pub fn ready_state(&self) -> RtcDataChannelState {
        self.channel.ready_state()
    }
    fn push_message(&self, message: Uint8Array) {
        let msg_len = message.length() as usize;
        self.msg_size.borrow_mut().push_back(msg_len);

        let mut copy_target = self.copy_target.borrow_mut();
        copy_target.clear();
        copy_target.extend((0..msg_len).map(|_| 0));
        message.copy_to(&mut copy_target);

        self.queue.borrow_mut().extend(copy_target.iter());
    }
    pub fn send_message(&self, message: &[u8]) -> Result<(), ()> {
        if !self.is_open() {
            return Err(());
        }
        self.channel.send_with_u8_array(message).map_err(|_| ())
    }
}

pub async fn start_channel_from_peer_listener(
    socket: WebSocketHandler,
    on_creation: impl FnMut(DataChannel) + Clone + 'static,
    mut on_request: impl FnMut(u64, &str) -> bool + 'static,
    mut should_close: impl FnMut(u32) -> bool + 'static,
    timeouts: ConnectionTimeouts,
) {
    let gatherer = MessageGatherer::new();
    socket.push_listener(gatherer.clone());
    let mut time = 0;
    let mut whitelist = Vec::new();
    loop {
        web_sleep(200).await;
        time += 200;
        if socket.socket.ready_state() != WebSocket::OPEN || (should_close)(time) {
            break;
        }
        while let Some((command, data)) = gatherer.pop_message() {
            let (socket_cmd::OFFER | socket_cmd::REQUEST_P2P) = command.as_str() else {
                continue;
            };
            let Ok((_, data)) = munch_u64(&data) else {
                continue;
            };
            let Ok((sender_id, data)) = munch_u64(data) else {
                continue;
            };
            if command.as_str() == socket_cmd::REQUEST_P2P {
                let is_valid = on_request(sender_id, data);
                if is_valid {
                    whitelist.push(sender_id);
                }
                let res = format!(
                    "{} {} {} {}",
                    socket_cmd::REQUEST_P2P_RES,
                    sender_id,
                    socket.id.get(),
                    is_valid
                );
                let _ = socket.socket.send_with_str(&res);
            } else if command.as_str() == socket_cmd::OFFER && whitelist.contains(&sender_id) {
                let offer = data.into();
                wasm_bindgen_futures::spawn_local(clone_move!(on_creation, socket => async move {
                    let mut on_creation = on_creation;
                    if let Ok(data_channel) = create_channel_from_peer_offer(sender_id, socket, offer, timeouts).await {
                        (on_creation)(data_channel);
                    }
                }));
            }
        }
    }
    let _ = socket.socket.close();
}

async fn create_channel_from_peer_offer(
    target_id: u64,
    socket: WebSocketHandler,
    offer: String,
    timeouts: ConnectionTimeouts,
) -> Result<DataChannel, RtcConnectionError> {
    let rtc_conn = create_rtc_connection()?;
    let channel = NullSmartPtr::<DataChannel>::null();

    let on_channel_received =
        Closure::<dyn FnMut(_)>::new(clone_move!(channel => move |e: RtcDataChannelEvent| {
            channel.set(DataChannel::new(e.channel()));
        }));
    rtc_conn.set_ondatachannel(Some(on_channel_received.as_ref().unchecked_ref()));
    on_channel_received.forget();

    set_remote_sdp(&rtc_conn, offer).await?;
    send_answer_sdp(
        &rtc_conn,
        &socket.socket,
        target_id,
        socket.id.get(),
        timeouts.ice_gathering,
    )
    .await?;
    wait_channel_open(
        rtc_conn.clone(),
        channel.clone(),
        timeouts.channel_connected,
    )
    .await?;

    let Ok(channel) = channel.try_unwrap() else {
        return Err(RtcConnectionError::RtcChannelNeverReceived);
    };
    Ok(channel)
}

async fn wait_channel_open(
    rtc_connection: RtcPeerConnection,
    channel: NullSmartPtr<DataChannel>,
    channel_open_timeout: u32,
) -> Result<(), RtcConnectionError> {
    wait_until(
        200,
        channel_open_timeout,
        RtcConnectionError::RtcConnectionTimeout,
        |time| {
            let rtc_state = rtc_connection.connection_state();
            if let RtcPeerConnectionState::Failed | RtcPeerConnectionState::Disconnected = rtc_state
            {
                return Err(RtcConnectionError::RtcConnectionFailed);
            }
            if let RtcIceConnectionState::Failed = rtc_connection.ice_connection_state() {
                return Err(RtcConnectionError::IceConnectionFailed);
            }
            if let Some(channel) = channel.borrow() {
                let ch_state = channel.ready_state();
                if rtc_state == RtcPeerConnectionState::Connected
                    || ch_state == RtcDataChannelState::Open
                {
                    log_fmt!("Wait time until creation - {}", time);
                    return Ok(Some(()));
                }
            }
            Ok(None)
        },
    )
    .await
}

pub async fn request_p2p_connection(
    target_id: u64,
    socket: WebSocketHandler,
    req_msg: &str,
    timeouts: ConnectionTimeouts,
) -> Result<bool, RtcConnectionError> {
    let gatherer = MessageGatherer::new();
    socket.push_listener(gatherer.clone());
    let _guard = gatherer.stop_guard();
    socket
        .socket
        .send_with_str(&format!("{} {}", socket_cmd::CHECK_ID, target_id))
        .map_err(|_| RtcConnectionError::WebSocketSend)?;
    wait_check_response(target_id, &gatherer, timeouts.socket_response).await?;
    socket
        .socket
        .send_with_str(&format!(
            "{} {} {} {}",
            socket_cmd::REQUEST_P2P,
            target_id,
            socket.id.get(),
            req_msg
        ))
        .map_err(|_| RtcConnectionError::WebSocketSend)?;
    wait_request_response(target_id, &gatherer, timeouts.socket_response).await
}

pub async fn create_channel_to_peer(
    target_id: u64,
    socket: WebSocketHandler,
    timeouts: ConnectionTimeouts,
) -> Result<DataChannel, RtcConnectionError> {
    let rtc_conn = create_rtc_connection()?;
    let channel = rtc_conn.create_data_channel("Data Channel");
    channel.set_binary_type(web_sys::RtcDataChannelType::Arraybuffer);
    let channel = DataChannel::new(channel);

    let gatherer = MessageGatherer::new();
    socket.push_listener(gatherer.clone());
    let _guard = gatherer.stop_guard();

    send_start_sdp(
        &rtc_conn,
        &socket.socket,
        target_id,
        socket.id.get(),
        timeouts.ice_gathering,
    )
    .await?;
    let answer = wait_for_answer(target_id, &gatherer, timeouts.answer_received).await?;
    set_remote_sdp(&rtc_conn, answer).await?;

    drop(_guard);

    wait_channel_open(
        rtc_conn.clone(),
        NullSmartPtr::new(channel.clone()),
        timeouts.channel_connected,
    )
    .await?;

    Ok(channel)
}

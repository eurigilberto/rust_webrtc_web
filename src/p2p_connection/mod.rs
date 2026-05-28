use std::collections::VecDeque;

use crate::{clone_move, log_fmt, utils::*, websocket::*};
use wasm_bindgen::prelude::*;
use web_sys::{js_sys::*, *};

mod inner;
use inner::*;

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
) {
    let gatherer = MessageGatherer::new();
    socket.push_listener(gatherer.clone());
    loop {
        web_sleep(200).await;
        if socket.socket.ready_state() != WebSocket::OPEN{
            break;
        }
        while let Some((command, data)) = gatherer.pop_message() {
            if command != socket_cmd::OFFER {
                continue;
            }
            let Ok((_, data)) = munch_u64(&data) else {
                continue;
            };
            let Ok((sender_id, offer)) = munch_u64(data) else {
                continue;
            };
            let offer = offer.into();
            wasm_bindgen_futures::spawn_local(clone_move!(on_creation, socket => async move {
                let mut on_creation = on_creation;
                if let Ok(data_channel) = create_channel_from_peer_offer(sender_id, socket, offer).await {
                    (on_creation)(data_channel);
                }
            }));
        }
    }
}

async fn create_channel_from_peer_offer(
    target_id: u64,
    socket: WebSocketHandler,
    offer: String,
) -> Result<DataChannel, ()> {
    log_fmt!("Offer received");
    let rtc_conn = create_rtc_connection();
    let channel = NullSmartPtr::<RtcDataChannel>::null();
    log_fmt!("Connection created");
    let on_channel_received = Closure::<dyn FnMut(_)>::new(
        clone_move!(channel => move |e: RtcDataChannelEvent| {
            log_fmt!("Channel received");
            channel.set(e.channel());
        }),
    );
    rtc_conn.set_ondatachannel(Some(on_channel_received.as_ref().unchecked_ref()));
    on_channel_received.forget();

    set_remote_sdp(&rtc_conn, offer).await?;
    log_fmt!("Remote offer set");
    send_answer_sdp(&rtc_conn, &socket.socket, target_id, socket.id.get()).await?;
    log_fmt!("Answer sent to remote");
    wait_channel_open(rtc_conn.clone(), channel.clone()).await?;

    let channel = channel.try_unwrap()?;
    Ok(DataChannel::new(channel))
}

async fn wait_channel_open(rtc_connection: RtcPeerConnection, channel: NullSmartPtr<RtcDataChannel>)->Result<(),()>{
    wait_until(200, 60000, || {
        log_fmt!("Connection state {:?}", rtc_connection.connection_state());
        if let Some(channel) = channel.borrow(){
            let ch_state = channel.ready_state();
            log_fmt!("Wait for channel open {:?}", ch_state);
            if ch_state == RtcDataChannelState::Open{
                return Ok(Some(()))
            }
        }
        Ok(None)
    })
    .await
}

pub async fn create_channel_to_peer(
    target_id: u64,
    socket: WebSocketHandler,
) -> Result<DataChannel, ()> {
    let rtc_conn = create_rtc_connection();
    log_fmt!("Connection created");
    let channel = rtc_conn.create_data_channel("Data Channel");
    channel.set_binary_type(web_sys::RtcDataChannelType::Arraybuffer);

    let ready_channel = SmartCell::new(false);
    let on_open = Closure::<dyn FnMut(Event)>::new(clone_move!(ready_channel => move |_|{
        ready_channel.set(true);
    }));
    channel.set_onopen(Some(on_open.as_ref().unchecked_ref()));

    let gatherer = MessageGatherer::new();
    socket.push_listener(gatherer.clone());
    log_fmt!("Gatherer added");
    check_other_peer_exists(target_id, &socket.socket, &gatherer).await?;
    log_fmt!("Check other");
    send_start_sdp(&rtc_conn, &socket.socket, target_id, socket.id.get()).await?;
    log_fmt!("Start offer");
    let answer = wait_for_answer(target_id, &gatherer).await?;
    log_fmt!("Got answer");
    set_remote_sdp(&rtc_conn, answer).await?;
    log_fmt!("Set remote offer");

    gatherer.stop();

    wait_channel_open(rtc_conn.clone(), NullSmartPtr::new(channel.clone())).await?;

    log_fmt!("Data channel created");
    Ok(DataChannel::new(channel))
}

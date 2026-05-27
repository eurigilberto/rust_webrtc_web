use std::collections::VecDeque;

use wasm_bindgen::prelude::*;
use web_sys::{js_sys::*, *};
use crate::{websocket::*, clone_move, utils::*};

mod inner;
use inner::*;

#[derive(Clone)]
pub struct DataChannel {
    channel: RtcDataChannel,
    is_open: SmartCell<bool>,
    copy_target: SmartPtr<Vec<u8>>,
    queue: SmartPtr<VecDeque<u8>>,
    msg_size: SmartPtr<VecDeque<usize>>
}

impl DataChannel{
    pub fn new(channel: RtcDataChannel)->Self{
        let is_open = SmartCell::new(true);
        let data_channel = Self{
            channel: channel.clone(),
            is_open: is_open.clone(),
            copy_target: SmartPtr::new(Vec::new()),
            queue: SmartPtr::new(VecDeque::new()),
            msg_size: SmartPtr::new(VecDeque::new())
        };
        channel.set_onopen(None);
        let on_message: Closure<dyn FnMut(_)> = Closure::new(clone_move!(data_channel => move |e: MessageEvent|{
            let buffer: ArrayBuffer = e.data().into();
            let buffer = Uint8Array::new(&buffer);
            data_channel.push_message(buffer);
        }));
        channel.set_onmessage(Some(on_message.as_ref().unchecked_ref()));
        let on_close: Closure<dyn FnMut(_)> = Closure::new(clone_move!(is_open => move |_: Event|{
            is_open.set(false);
        }));
        channel.set_onclose(Some(on_close.as_ref().unchecked_ref()));
        data_channel
    }
    pub fn is_open(&self)->bool{
        self.is_open.get()
    }
    pub fn pop_message(&self, message: &mut Vec<u8>){
        let Some(size) = self.msg_size.borrow_mut().pop_front() else {
            return;
        };
        message.extend(self.queue.borrow_mut().drain(..size));
    }
    fn push_message(&self, message: Uint8Array){
        let msg_len = message.length() as usize;
        self.msg_size.borrow_mut().push_back(msg_len);
        
        let mut copy_target = self.copy_target.borrow_mut();
        copy_target.clear();
        copy_target.extend((0..msg_len).map(|_|0));
        message.copy_to(&mut copy_target);

        self.queue.borrow_mut().extend(copy_target.iter());
    }
    pub fn send_message(&self, message: &[u8])->Result<(), ()>{
        if !self.is_open() {
            return Err(())
        }
        self.channel.send_with_u8_array(message).map_err(|_| ())
    }
}

pub async fn create_channel_from_peer_offer(
    target_id: u64,
    socket: WebSocketHandler,
    offer: String,
) -> Result<DataChannel, ()> {
    let rtc_conn = create_rtc_connection();
    let channel = NullSmartPtr::<RtcDataChannel>::null();

    let ready_channel = SmartCell::new(false);
    let on_channel_received = Closure::<dyn FnMut(_)>::new(
        clone_move!(ready_channel, channel => move |e: RtcDataChannelEvent| {
            let on_open = Closure::<dyn FnMut(Event)>::new(clone_move!(ready_channel => move |_|{
                ready_channel.set(true);
            }));
            e.channel().set_onopen(Some(on_open.as_ref().unchecked_ref()));
            channel.set(e.channel());
        }),
    );
    rtc_conn.set_ondatachannel(Some(on_channel_received.as_ref().unchecked_ref()));
    on_channel_received.forget();

    set_remote_offer(&rtc_conn, offer).await?;
    send_answer_offer(&rtc_conn, &socket.socket, target_id, socket.id.get()).await?;

    wait_until(200, 2000, || {
        if ready_channel.get() {
            return Ok(Some(()));
        }
        Ok(None)
    })
    .await?;

    let channel = channel.try_unwrap()?;
    Ok(DataChannel::new(channel))
}

pub async fn create_channel_to_peer(
    target_id: u64,
    socket: WebSocketHandler,
) -> Result<DataChannel, ()> {
    let rtc_conn = create_rtc_connection();
    let channel = rtc_conn.create_data_channel("Data Channel");
    channel.set_binary_type(web_sys::RtcDataChannelType::Arraybuffer);

    let ready_channel = SmartCell::new(false);
    let on_open = Closure::<dyn FnMut(Event)>::new(clone_move!(ready_channel => move |_|{
        ready_channel.set(true);
    }));
    channel.set_onopen(Some(on_open.as_ref().unchecked_ref()));

    let gatherer = MessageGatherer::new();
    socket.push_listener(gatherer.clone());

    check_other_peer_exists(target_id, &gatherer).await?;
    send_start_offer(&rtc_conn, &socket.socket, target_id, socket.id.get()).await?;
    let answer = wait_for_answer(target_id, &gatherer).await?;
    set_remote_offer(&rtc_conn, answer).await?;

    gatherer.stop();

    wait_until(200, 2000, || {
        if ready_channel.get() {
            return Ok(Some(()));
        }
        Ok(None)
    })
    .await?;

    Ok(DataChannel::new(channel))
}
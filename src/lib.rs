use std::str::FromStr;
use wasm_bindgen::prelude::*;
use web_sys::{js_sys::*, *};

mod websocket;
pub use websocket::*;
mod utils;
pub use utils::*;

pub struct DataChannel{
    channel: RtcDataChannel,

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

pub async fn create_channel_from_peer_offer(
    target_id: u64,
    socket: WebSocketHandler,
    offer: String,
) -> Result<DataChannel, ()> {
    let rtc_conn = create_rtc_connection();
    let channel = NullSmartPtr::<RtcDataChannel>::null();

    let ready_channel = SmartCell::new(false);
    let on_channel_received =
        Closure::<dyn FnMut(_)>::new(clone_move!(ready_channel, channel => move |e: RtcDataChannelEvent| {
            let on_open = Closure::<dyn FnMut(Event)>::new(clone_move!(ready_channel => move |_|{
                ready_channel.set(true);
            }));
            e.channel().set_onopen(Some(on_open.as_ref().unchecked_ref()));
            channel.set(e.channel());
        }));
    rtc_conn.set_ondatachannel(Some(on_channel_received.as_ref().unchecked_ref()));
    on_channel_received.forget();

    set_remote_offer(&rtc_conn, offer).await;
    let Ok(answer) = rtc_conn.create_answer().await else {
        return Err(());
    };
    let answer: RtcSessionDescriptionInit = answer.into();
    let Ok(_) = rtc_conn.set_local_description(&answer).await else {
        return Err(());
    };
    let Ok(_) = wait_for_ice_candidates(&rtc_conn).await else {
        return Err(());
    };
    send_offer(
        &rtc_conn,
        &socket.socket,
        "#ANSWER",
        target_id,
        socket.id.get(),
    )?;

    wait_until(200, 2000, || {
        if ready_channel.get() {
            return Ok(Some(()));
        }
        Ok(None)
    })
    .await?;

    todo!()
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
    set_remote_offer(&rtc_conn, answer).await;

    gatherer.stop();

    wait_until(200, 2000, || {
        if ready_channel.get() {
            return Ok(Some(()));
        }
        Ok(None)
    })
    .await?;

    todo!()
}

async fn set_remote_offer(conn: &RtcPeerConnection, offer: String) {
    let remote_offer: JsValue = js_sys::JSON::parse(&offer).expect("Could not parse offer");
    let remote_offer: RtcSessionDescriptionInit = remote_offer.into();
    conn.set_remote_description(&remote_offer)
        .await
        .expect("Could not set remote description");
}

async fn check_other_peer_exists(target_id: u64, gatherer: &MessageGatherer) -> Result<(), ()> {
    let check_target = clone_move!(gatherer => move ||{
        for (command, data) in gatherer.drain(){
            if command != "#CHECK_ID_RES"{
                return Ok(None)
            }
            let (id, data) = munch_u64(&data)?;
            if id == target_id {
                return Ok(Some(data.trim() == "true"));
            }
        }
        return Ok(None)
    });
    let Ok(true) = wait_until(100, 2000, check_target).await else {
        return Err(());
    };
    Ok(())
}

fn send_offer(
    rtc_conn: &RtcPeerConnection,
    socket: &WebSocket,
    command: &str,
    target_id: u64,
    sender_id: u64,
) -> Result<(), ()> {
    let Some(offer) = rtc_conn.local_description() else {
        return Err(());
    };
    let offer = js_sys::JSON::stringify(&offer).unwrap();
    let offer = offer.as_string().unwrap();
    let offer = format!("{} {} {} {}", command, target_id, sender_id, offer);
    let Ok(_) = socket.send_with_str(&offer) else {
        return Err(());
    };
    Ok(())
}

async fn send_start_offer(
    rtc_conn: &RtcPeerConnection,
    socket: &WebSocket,
    target_id: u64,
    sender_id: u64,
) -> Result<(), ()> {
    let Ok(offer) = rtc_conn.create_offer().await else {
        return Err(());
    };
    let offer: RtcSessionDescriptionInit = offer.into();
    let Ok(_) = rtc_conn.set_local_description(&offer).await else {
        return Err(());
    };
    let Ok(_) = wait_for_ice_candidates(&rtc_conn).await else {
        return Err(());
    };

    send_offer(rtc_conn, socket, "#OFFER", target_id, sender_id)
}

async fn wait_for_answer(target_id: u64, gatherer: &MessageGatherer) -> Result<String, ()> {
    let check_target = clone_move!(gatherer => move ||{
        for (command, data) in gatherer.drain(){
            if command != "#ANSWER"{
                return Ok(None)
            }
            let (_, data) = munch_u64(&data)?;
            let (sender_id, data) = munch_u64(data)?;
            if sender_id == target_id {
                let data = String::from_str(data.trim()).unwrap();
                return Ok(Some(data));
            }
        }
        return Ok(None)
    });
    let Ok(answer) = wait_until(100, 2000, check_target).await else {
        return Err(());
    };
    Ok(answer)
}

async fn wait_for_ice_candidates(conn: &RtcPeerConnection) -> Result<(), ()> {
    let gathered_candidates = || {
        if conn.ice_gathering_state() == RtcIceGatheringState::Complete {
            return Ok(Some(()));
        }
        return Ok(None);
    };
    wait_until(200, 3000, gathered_candidates).await
}

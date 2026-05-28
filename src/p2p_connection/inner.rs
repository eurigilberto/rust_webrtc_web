use wasm_bindgen::prelude::*;
use web_sys::{js_sys::*, *};

use crate::{clone_move, log_fmt, utils::*, websocket::*};

pub(crate) fn create_rtc_connection() -> RtcPeerConnection {
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

pub(crate) async fn send_answer_sdp(
    rtc_conn: &RtcPeerConnection,
    socket: &WebSocket,
    target_id: u64,
    sender_id: u64,
) -> Result<(), ()> {
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
    send_offer(&rtc_conn, socket, socket_cmd::ANSWER, target_id, sender_id)?;
    Ok(())
}

pub(crate) async fn set_remote_sdp(conn: &RtcPeerConnection, offer: String) -> Result<(), ()> {
    let remote_offer: JsValue = js_sys::JSON::parse(&offer).expect("Could not parse offer");
    let remote_offer: RtcSessionDescriptionInit = remote_offer.into();
    let Ok(_) = conn.set_remote_description(&remote_offer).await else {
        return Err(());
    };
    Ok(())
}

pub(crate) async fn check_other_peer_exists(target_id: u64, socket: &WebSocket, gatherer: &MessageGatherer) -> Result<(), ()> {
    socket.send_with_str(&format!("{} {}",socket_cmd::CHECK_ID, target_id)).map_err(|_|())?;
    let check_target = clone_move!(gatherer => move ||{
        while let Some((command, data)) = gatherer.pop_message(){
            if command != socket_cmd::CHECK_ID_RES{
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
    gatherer.clear_messages();
    Ok(())
}

pub(crate) fn send_offer(
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

pub(crate) async fn send_start_sdp(
    rtc_conn: &RtcPeerConnection,
    socket: &WebSocket,
    target_id: u64,
    sender_id: u64,
) -> Result<(), ()> {
    log_fmt!("wait for create offer");
    let Ok(offer) = rtc_conn.create_offer().await else {
        return Err(());
    };
    let offer: RtcSessionDescriptionInit = offer.into();
    log_fmt!("wait for set local desc");
    let Ok(_) = rtc_conn.set_local_description(&offer).await else {
        return Err(());
    };
    log_fmt!("wait for ice candidates");
    let Ok(_) = wait_for_ice_candidates(&rtc_conn).await else {
        return Err(());
    };

    send_offer(rtc_conn, socket, socket_cmd::OFFER, target_id, sender_id)
}

pub(crate) async fn wait_for_answer(target_id: u64, gatherer: &MessageGatherer) -> Result<String, ()> {
    let check_target = clone_move!(gatherer => move ||{
        log_fmt!("Wait for answer");
        while let Some((command, data)) = gatherer.pop_message(){
            if command != socket_cmd::ANSWER{
                return Ok(None)
            }
            let (_, data) = munch_u64(&data)?;
            let (sender_id, data) = munch_u64(data)?;
            if sender_id == target_id {
                let data: String = data.trim().into();
                return Ok(Some(data));
            }
        }
        return Ok(None)
    });
    let Ok(answer) = wait_until(200, 30000, check_target).await else {
        return Err(());
    };
    gatherer.clear_messages();
    Ok(answer)
}

pub(crate) async fn wait_for_ice_candidates(conn: &RtcPeerConnection) -> Result<(), ()> {
    let gathered_candidates = || {
        if conn.ice_gathering_state() == RtcIceGatheringState::Complete {
            return Ok(Some(()));
        }
        return Ok(None);
    };
    let _ = wait_until(200, 10000, gathered_candidates).await;
    Ok(())
}
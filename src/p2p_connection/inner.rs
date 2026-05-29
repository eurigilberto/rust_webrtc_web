use wasm_bindgen::prelude::*;
use web_sys::{js_sys::*, *};

use crate::{clone_move, log_fmt, p2p_connection::RtcConnectionError, utils::*, websocket::*};

pub(crate) fn create_rtc_connection() -> Result<RtcPeerConnection, RtcConnectionError> {
    let configuration = RtcConfiguration::new();
    let servers = [
        js_sys::JSON::parse("{\"urls\": \"stun:stun.l.google.com:19302\"}").unwrap(),
        js_sys::JSON::parse("{\"urls\": \"stun:stun1.l.google.com:19302\"}").unwrap(),
        js_sys::JSON::parse("{\"urls\": \"stun:stun2.l.google.com:19302\"}").unwrap(),
        js_sys::JSON::parse("{\"urls\": \"stun:stun3.l.google.com:19302\"}").unwrap(),
        js_sys::JSON::parse("{\"urls\": \"stun:stun4.l.google.com:19302\"}").unwrap(),
    ];
    let ice_servers = js_sys::Array::new();
    for server in servers {
        ice_servers.push(&server);
    }
    configuration.set_ice_servers(&ice_servers);
    RtcPeerConnection::new_with_configuration(&configuration).map_err(|_| RtcConnectionError::RtcConnectionInit)
}

pub(crate) async fn send_answer_sdp(
    rtc_conn: &RtcPeerConnection,
    socket: &WebSocket,
    target_id: u64,
    sender_id: u64,
    ice_timeout: u32,
) -> Result<(), RtcConnectionError> {
    let Ok(answer) = rtc_conn.create_answer().await else {
        return Err(RtcConnectionError::AnswerCreation);
    };
    let answer: RtcSessionDescriptionInit = answer.into();
    let Ok(_) = rtc_conn.set_local_description(&answer).await else {
        return Err(RtcConnectionError::SetLocalDescription);
    };
    wait_for_ice_candidates(&rtc_conn, ice_timeout).await;

    let Some(offer) = rtc_conn.local_description() else {
        return Err(RtcConnectionError::SetLocalDescription);
    };
    send_offer(offer, socket, socket_cmd::ANSWER, target_id, sender_id)?;
    Ok(())
}

pub(crate) async fn set_remote_sdp(conn: &RtcPeerConnection, offer: String) -> Result<(), RtcConnectionError> {
    let remote_offer: JsValue = js_sys::JSON::parse(&offer).expect("Could not parse offer");
    let remote_offer: RtcSessionDescriptionInit = remote_offer.into();
    let Ok(_) = conn.set_remote_description(&remote_offer).await else {
        return Err(RtcConnectionError::SetRemoteDescription);
    };
    Ok(())
}

pub(crate) async fn wait_check_response(
    target_id: u64,
    gatherer: &MessageGatherer,
    timeout: u32,
) -> Result<(), RtcConnectionError> {
    let check_target = clone_move!(gatherer => move |_|{
        let Some((_, data)) = gatherer.pop_messages_until(socket_cmd::CHECK_ID_RES) else {
            return Ok(None);
        };
        let Ok((id, data)) = munch_u64(&data) else {
            return Err(RtcConnectionError::IdParsingError)
        };
        if id == target_id {
            return Ok(Some(data.trim() == "true"));
        }
        return Ok(None)
    });
    let check_response = wait_until(100, timeout, RtcConnectionError::CheckResponseTimeout, check_target).await?;
    gatherer.clear_messages();
    if !check_response {
        return Err(RtcConnectionError::PeerDoesNotExist)
    }
    Ok(())
}

pub(crate) async fn wait_request_response(
    target_id: u64,
    gatherer: &MessageGatherer,
    timeout: u32,
) -> Result<bool, RtcConnectionError> {
    let check_target = clone_move!(gatherer => move |_|{
        let Some((_, data)) = gatherer.pop_messages_until(socket_cmd::REQUEST_P2P_RES) else {
            return Ok(None);
        };
        let Ok((_, data)) = munch_u64(&data) else {
            return Err(RtcConnectionError::IdParsingError)
        };
        let Ok((sender_id, data)) = munch_u64(&data) else {
            return Err(RtcConnectionError::IdParsingError)
        };
        if sender_id == target_id {
            return Ok(Some(data.trim() == "true"));
        }
        return Ok(None)
    });
    wait_until(100, timeout, RtcConnectionError::RequestP2PResponseTimeout, check_target).await
}


pub(crate) fn send_offer(
    rtc_sdp: RtcSessionDescription,
    socket: &WebSocket,
    command: &str,
    target_id: u64,
    sender_id: u64,
) -> Result<(), RtcConnectionError> {
    let offer = js_sys::JSON::stringify(&rtc_sdp).unwrap();
    let offer = offer.as_string().unwrap();
    let offer = format!("{} {} {} {}", command, target_id, sender_id, offer);
    let Ok(_) = socket.send_with_str(&offer) else {
        return Err(RtcConnectionError::WebSocketSend);
    };
    Ok(())
}

pub(crate) async fn send_start_sdp(
    rtc_conn: &RtcPeerConnection,
    socket: &WebSocket,
    target_id: u64,
    sender_id: u64,
    ice_timeout: u32,
) -> Result<(), RtcConnectionError> {
    let Ok(offer) = rtc_conn.create_offer().await else {
        return Err(RtcConnectionError::OfferCreation);
    };
    let offer: RtcSessionDescriptionInit = offer.into();
    
    let Ok(_) = rtc_conn.set_local_description(&offer).await else {
        return Err(RtcConnectionError::SetLocalDescription);
    };
    wait_for_ice_candidates(&rtc_conn, ice_timeout).await;

    let Some(offer) = rtc_conn.local_description() else {
        return Err(RtcConnectionError::OfferSDPCreation);
    };
    
    send_offer(offer, socket, socket_cmd::OFFER, target_id, sender_id)
}

pub(crate) async fn wait_for_answer(
    target_id: u64,
    gatherer: &MessageGatherer,
    answer_timeout: u32,
) -> Result<String, RtcConnectionError> {
    let check_target = clone_move!(gatherer => move |_|{
        while let Some((command, data)) = gatherer.pop_message(){
            if command != socket_cmd::ANSWER{
                return Ok(None)
            }
            let Ok((_, data)) = munch_u64(&data) else {
                return Err(RtcConnectionError::IdParsingError)
            };
            let Ok((sender_id, data)) = munch_u64(data) else {
                return Err(RtcConnectionError::IdParsingError)
            };
            if sender_id == target_id {
                let data: String = data.trim().into();
                return Ok(Some(data));
            }
        }
        return Ok(None)
    });
    wait_until(200, answer_timeout, RtcConnectionError::WaitAnswerTimeout, check_target).await
}

pub(crate) async fn wait_for_ice_candidates(
    conn: &RtcPeerConnection,
    ice_timeout: u32,
) {
    let gathered_candidates = |_| {
        if conn.ice_gathering_state() == RtcIceGatheringState::Complete {
            return Ok(Some(()));
        }
        return Ok(None);
    };
    if wait_until(200, ice_timeout, (), gathered_candidates).await.is_err() {
        log_fmt!("Wait for ICE Gathering Timedout - {}", ice_timeout);
    }
}

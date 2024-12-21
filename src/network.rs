use anyhow::Result;
use futures::SinkExt as _;
use tokio::net::TcpStream;
use tokio_stream::StreamExt as _;
use tokio_util::codec::{Decoder, Encoder, Framed};
use tracing::info;

use crate::{
    cmd::{Command, CommandExecutor},
    Backend, RespDecode, RespEncode, RespError, RespFrame,
};

#[derive(Debug)]
struct RedisRequest {
    frame: RespFrame,
    backend: Backend,
}

#[derive(Debug)]
struct RedisResponse {
    frame: RespFrame,
}

#[derive(Debug)]
struct RespFrameCodec;

pub async fn stream_handler(stream: TcpStream, backend: Backend) -> Result<()> {
    let mut framed = Framed::new(stream, RespFrameCodec);

    loop {
        match framed.next().await {
            Some(Ok(frame)) => {
                info!("received frame: {:?}", frame);
                let request = RedisRequest {
                    frame,
                    backend: backend.clone(),
                };
                let response = handle_request(request).await?;
                info!("Sending response: {:?}", response.frame);
                framed.send(response.frame).await?;
            }
            Some(Err(e)) => return Err(e),
            None => return Ok(()),
        }
    }
}

async fn handle_request(request: RedisRequest) -> Result<RedisResponse> {
    let (frame, backend) = (request.frame, request.backend);
    let cmd = Command::try_from(frame)?;
    info!("Executing command: {:?}", cmd);
    let resp_frame = cmd.execute(&backend);
    Ok(RedisResponse { frame: resp_frame })
}

impl Decoder for RespFrameCodec {
    type Item = RespFrame;

    type Error = anyhow::Error;

    fn decode(
        &mut self,
        src: &mut bytes::BytesMut,
    ) -> std::result::Result<Option<Self::Item>, Self::Error> {
        match RespFrame::decode(src) {
            Ok(frame) => Ok(Some(frame)),
            Err(RespError::Incomplete) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }
}

impl Encoder<RespFrame> for RespFrameCodec {
    type Error = anyhow::Error;

    fn encode(
        &mut self,
        item: RespFrame,
        dst: &mut bytes::BytesMut,
    ) -> std::result::Result<(), Self::Error> {
        let encoded = item.encode();
        dst.extend_from_slice(&encoded);
        Ok(())
    }
}

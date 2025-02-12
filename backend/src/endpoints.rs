use crate::{
    authrequest::AuthRequest, channelmessage::ChannelMessage, context::Context, response::Response,
    userconnection::UserConnection,
};
use anyhow::Result;
use chrono::Utc;
use futures_util::StreamExt;
use jwt_simple::{
    claims::{Claims, NoCustomClaims},
    prelude::{Duration, MACLike},
};
use tokio::{io::AsyncWriteExt, net::TcpStream};
use tokio_tungstenite::{accept_async, tungstenite::Message};

pub async fn signin(context: Context, mut socket: TcpStream, body: &str) -> Result<()> {
    let authrequest: AuthRequest = serde_json::from_str(body)?;
    if !authrequest.is_valid() {
        let _ = socket
            .write(&Response::new(409, "Conflict", "Wrong format"))
            .await?;
        return Ok(());
    }
    let query = sqlx::query("SELECT * FROM users WHERE (username = $1) AND (password = $2)")
        .bind(authrequest.username)
        .bind(authrequest.password)
        .execute(&context.pg_pool)
        .await?;
    if query.rows_affected() != 0 {
        if !context.contains(authrequest.username).await {
            let claims = Claims::create(Duration::from_mins(1)).with_subject(authrequest.username);
            let token = context.key().authenticate(claims)?;
            let _ = socket.write(&Response::new(200, "OK", &token)).await?;
        } else {
            let _ = socket
                .write(&Response::new(409, "Conflict", "Already in"))
                .await?;
        }
    } else {
        let _ = socket
            .write(&Response::new(
                401,
                "Unauthorized",
                "Wrong username/password",
            ))
            .await?;
    }
    Ok(())
}

pub async fn signup(context: Context, mut socket: TcpStream, body: &str) -> Result<()> {
    let authrequest: AuthRequest = serde_json::from_str(body)?;
    if !authrequest.is_valid() {
        let _ = socket
            .write(&Response::new(409, "Conflict", "Wrong format"))
            .await?;
        return Ok(());
    }
    let query = sqlx::query("SELECT * FROM users WHERE username = $1")
        .bind(authrequest.username)
        .execute(&context.pg_pool)
        .await?;
    if query.rows_affected() == 0 {
        let _ = socket.write(&Response::new(200, "OK", "Success")).await?;
        sqlx::query("INSERT INTO users (username, password) VALUES ($1, $2)")
            .bind(authrequest.username)
            .bind(authrequest.password)
            .execute(&context.pg_pool)
            .await?;
    } else {
        let _ = socket
            .write(&Response::new(409, "Conflict", "Already exists"))
            .await?;
    }
    Ok(())
}

pub async fn connect(mut context: Context, socket: TcpStream, headers: &str) -> Result<()> {
    if headers.contains("Upgrade: websocket") {
        let mut ws_stream = accept_async(socket).await?;
        if let Some(Ok(Message::Text(token))) = ws_stream.next().await {
            match context.key().verify_token::<NoCustomClaims>(&token, None) {
                Ok(claims) => {
                    let username = claims.subject.unwrap();
                    let (ws_tx, mut ws_rx) = ws_stream.split();
                    let connection = UserConnection::new(ws_tx).await;
                    context.plug(&username, connection).await?;
                    while let Some(Ok(Message::Text(msg))) = ws_rx.next().await {
                        context.channel.send(ChannelMessage {
                            from: username.clone(),
                            body: msg,
                            timestamp: Utc::now().timestamp_millis(),
                        })?
                    }
                    context.unplug(&username).await;
                }
                Err(_) => ws_stream.close(None).await?,
            }
        }
    }
    Ok(())
}

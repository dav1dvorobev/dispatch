use crate::{channelmessage::ChannelMessage, context::ConnectionsPool};
use anyhow::Result;
use sqlx::PgPool;
use tokio::sync::mpsc;

pub type ChannelSender = mpsc::UnboundedSender<ChannelMessage>;

#[derive(Clone)]
pub struct Channel {
    sender: ChannelSender,
}
impl Channel {
    pub async fn new(pg_pool: PgPool, connections: ConnectionsPool) -> Result<Self> {
        let (sender, mut receiver) = mpsc::unbounded_channel::<ChannelMessage>();
        tokio::spawn(async move {
            while let Some(msg) = receiver.recv().await {
                for (_, connection) in connections.read().await.iter() {
                    connection.send(msg.clone());
                }
                sqlx::query(
                    "INSERT INTO messages (\"from\", \"body\", \"timestamp\") VALUES ($1, $2, $3)",
                )
                .bind(&msg.from)
                .bind(&msg.body)
                .bind(msg.timestamp)
                .execute(&pg_pool)
                .await
                .expect("failed insert into to table \"messages\"");
            }
        });
        Ok(Self { sender })
    }
    pub fn send(&self, msg: ChannelMessage) -> Result<()> {
        self.sender.send(msg)?;
        Ok(())
    }
}

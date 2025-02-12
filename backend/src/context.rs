use crate::{channel::Channel, channelmessage::ChannelMessage, userconnection::UserConnection};
use anyhow::Result;
use jwt_simple::prelude::HS256Key;
use sqlx::{migrate, postgres::PgPoolOptions, PgPool};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::RwLock;

pub type ConnectionsPool = Arc<RwLock<HashMap<String, UserConnection>>>;

#[derive(Clone)]
pub struct Context {
    key: HS256Key,
    pub pg_pool: PgPool,
    pub channel: Channel,
    pub connections: ConnectionsPool,
}
impl Context {
    pub async fn create() -> Result<Self> {
        let key = HS256Key::generate();
        let database_url = std::env::var("DATABASE_URL")?;
        let pg_pool = PgPoolOptions::new()
            .max_connections(10)
            .connect(&database_url)
            .await?;
        migrate!().run(&pg_pool).await?;
        let connections = Arc::new(RwLock::new(HashMap::new()));
        let channel = Channel::new(pg_pool.clone(), connections.clone()).await?;
        Ok(Self {
            key,
            pg_pool,
            channel,
            connections,
        })
    }
    pub fn key(&self) -> &HS256Key {
        &self.key
    }
    pub async fn contains(&self, username: &str) -> bool {
        self.connections.read().await.contains_key(username)
    }
    pub async fn plug(&mut self, username: &str, connection: UserConnection) -> Result<()> {
        let mut lock = self.connections.write().await;
        let preload = sqlx::query_as::<_, ChannelMessage>(
            "SELECT \"from\", \"body\", \"timestamp\" FROM messages",
        )
        .fetch_all(&self.pg_pool)
        .await?;
        preload.into_iter().for_each(|msg| connection.send(msg));
        lock.insert(username.to_string(), connection);
        Ok(())
    }
    pub async fn unplug(&mut self, username: &str) {
        self.connections
            .write()
            .await
            .remove(username)
            .unwrap()
            .close();
    }
}

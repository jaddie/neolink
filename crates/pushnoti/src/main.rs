use anyhow::{Context, Result};
use clap::Parser;
use log::*;
use std::{fs, path::PathBuf};
use tokio::io::AsyncWriteExt;
use tokio_stream::StreamExt;
use validator::Validate;

mod config;
mod opt;
mod utils;

use config::Config;
use opt::Opt;
use utils::find_and_connect;

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let opt = Opt::parse();

    let firebase_app_id = "1:743639030586:android:86f60a4fb7143876";
    let firebase_project_id = "reolink-login";
    let firebase_api_key = "AIzaSyBEUIuWHnnOEwFahxWgQB4Yt4NsgOmkPyE";
    let http = reqwest::Client::new();

    let token_path = PathBuf::from("./token.toml");
    let mut registration = if let Ok(Ok(registration)) = fs::read_to_string(&token_path)
        .map(|v| toml::from_str::<fcm_push_listener::Registration>(&v))
    {
        info!("Loaded token");
        registration
    } else {
        info!("Registering new token");
        let registration = fcm_push_listener::register(
            &http,
            firebase_app_id,
            firebase_project_id,
            firebase_api_key,
            None,
        )
        .await?;
        let new_token = toml::to_string(&registration)?;
        fs::write(&token_path, new_token)?;
        registration
    };

    if opt.register_only {
        info!("registration.fcm_token: {}", registration.fcm_token);
        return Ok(());
    }

    let conf_path = opt.config.context("Must supply --config file")?;
    let camera_name = opt.camera.context("Must supply camera name")?;
    let config: Config = toml::from_str(
        &fs::read_to_string(&conf_path)
            .with_context(|| format!("Failed to read {:?}", conf_path))?,
    )
    .with_context(|| format!("Failed to parse the {:?} config file", conf_path))?;

    config
        .validate()
        .with_context(|| format!("Failed to validate the {:?} config file", conf_path))?;

    let camera = find_and_connect(&config, &camera_name).await?;

    // Send registration.fcm_token to the server to allow it to send push messages to you.
    info!("registration.fcm_token: {}", registration.fcm_token);
    let uid = "6A5443E486511B0D828543445DC55A7D"; // MD5 Hash of "WHY_REOLINK"
    camera
        .send_pushinfo_android(&registration.fcm_token, uid)
        .await?;

    info!("Listening");
    let session = registration.gcm.checkin(&http).await?;
    if session.changed(&registration.gcm) {
        registration.gcm = (*session).clone();
        fs::write(&token_path, toml::to_string(&registration)?)?;
    }

    let connection = session.new_connection(vec![]).await?;
    let mut stream = fcm_push_listener::MessageStream::wrap(connection, &registration.keys);
    while let Some(message) = stream.next().await {
        match message? {
            fcm_push_listener::Message::Data(message) => {
                info!("Message JSON: {}", String::from_utf8_lossy(&message.body));
                info!("Persistent ID: {:?}", message.persistent_id);
            }
            fcm_push_listener::Message::HeartbeatPing => {
                stream
                    .write_all(&fcm_push_listener::new_heartbeat_ack())
                    .await?;
            }
            fcm_push_listener::Message::Other(tag, bytes) => {
                debug!("Got non-data message: {}, {} bytes", tag, bytes.len());
            }
        }
    }
    Ok(())
}

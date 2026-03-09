//! This thread handles push notifications
//! the last notification is pushed into a watcher
//! as is, which comes fromt the json structure
//!

use anyhow::Context;
use std::collections::{HashMap, HashSet};
use std::{
    fs,
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::{
    io::AsyncWriteExt,
    sync::{
        mpsc::{Receiver as MpscReceiver, Sender as MpscSender},
        oneshot::Sender as OneshotSender,
        watch::{channel as watch, Receiver as WatchReceiver, Sender as WatchSender},
        RwLock,
    },
    time::{sleep, timeout, Duration},
};
use tokio_stream::StreamExt;

use super::NeoInstance;
use crate::AnyResult;

const FIREBASE_APP_ID: &str = "1:743639030586:android:86f60a4fb7143876";
const FIREBASE_PROJECT_ID: &str = "reolink-login";
const FIREBASE_API_KEY: &str = "AIzaSyBEUIuWHnnOEwFahxWgQB4Yt4NsgOmkPyE";
const MAX_RECEIVED_IDS: usize = 1024;

pub(crate) struct PushNotiThread {
    pn_watcher: Arc<WatchSender<Option<PushNoti>>>,
    registed_cameras: HashMap<String, NeoInstance>,
    received_ids: Arc<RwLock<HashSet<String>>>,
}

// The push notification
#[derive(Clone, Eq, PartialEq, Debug)]
pub(crate) struct PushNoti {
    pub(crate) message: String,
    pub(crate) id: Option<String>,
}

pub(crate) enum PnRequest {
    Get {
        sender: OneshotSender<WatchReceiver<Option<PushNoti>>>,
    },
    Activate {
        instance: NeoInstance,
        sender: OneshotSender<AnyResult<()>>,
    },
    AddPushID {
        id: String,
    },
}

impl PushNotiThread {
    pub(crate) async fn new() -> AnyResult<Self> {
        let (pn_watcher, _) = watch(None);

        Ok(PushNotiThread {
            pn_watcher: Arc::new(pn_watcher),
            registed_cameras: Default::default(),
            received_ids: Arc::new(RwLock::new(Default::default())),
        })
    }

    pub(crate) async fn run(
        &mut self,
        sender: &MpscSender<PnRequest>,
        pn_request_rx: &mut MpscReceiver<PnRequest>,
    ) -> AnyResult<()> {
        loop {
            // Short wait on start/retry
            sleep(Duration::from_secs(3)).await;

            let token_path = token_path();
            log::debug!("Push notification details are saved to {:?}", token_path);

            let http = reqwest::Client::new();
            let mut registration = if let Some(Ok(Ok(registration))) = token_path
                .as_ref()
                .map(|token_path| read_registration(token_path.as_path()))
            {
                log::debug!("Loaded push notification token");
                registration
            } else {
                log::debug!("Registering new push notification token");
                match fcm_push_listener::register(
                    &http,
                    FIREBASE_APP_ID,
                    FIREBASE_PROJECT_ID,
                    FIREBASE_API_KEY,
                    None,
                )
                .await
                {
                    Ok(registration) => {
                        save_registration(token_path.as_deref(), &registration);
                        registration
                    }
                    Err(e) => {
                        log::warn!("Issue connecting to push notifications server: {:?}", e);
                        continue;
                    }
                }
            };

            // Send registration.fcm_token to the server to allow it to send push messages to you.
            log::debug!("registration.fcm_token: {}", registration.fcm_token);
            let md5ed = md5::compute(format!("WHY_REOLINK_{:?}", registration.fcm_token));
            let uid = format!("{:X}", md5ed);
            let fcm_token = registration.fcm_token.clone();
            log::debug!("push notification UID: {}", uid);

            log::debug!("Push notification Listening");
            let thread_pn_watcher = self.pn_watcher.clone();

            for (_, instance) in self.registed_cameras.iter() {
                let uid = uid.clone();
                let fcm_token = fcm_token.clone();
                let instance = instance.clone();
                tokio::task::spawn(async move {
                    let _ = instance
                        .run_task(|camera| {
                            let fcm_token = fcm_token.clone();
                            let uid = uid.clone();
                            Box::pin(async move {
                                let r = camera.send_pushinfo_android(&fcm_token, &uid).await;
                                log::debug!(
                                    "Registered {} for push notifications: {:?}",
                                    camera.uid().await?,
                                    r
                                );
                                r?;
                                AnyResult::Ok(())
                            })
                        })
                        .await;
                });
            }

            let received_ids = self.received_ids.clone();
            let _ = tokio::select! {
                v = async {
                    let v: AnyResult<()> = async {
                        loop {
                            let checked_session = registration.gcm.checkin(&http).await?;
                            if checked_session.changed(&registration.gcm) {
                                registration.gcm = (*checked_session).clone();
                                save_registration(token_path.as_deref(), &registration);
                            }

                            let connection = checked_session
                                .new_connection(received_ids.read().await.iter().cloned().collect())
                                .await?;
                            let mut stream = fcm_push_listener::MessageStream::wrap(
                                connection,
                                &registration.keys,
                            );

                            let r: Result<AnyResult<()>, tokio::time::error::Elapsed> =
                                timeout(Duration::from_secs(60 * 5), async {
                                    while let Some(message) = stream.next().await {
                                        match message? {
                                            fcm_push_listener::Message::Data(message) => {
                                                let payload_json =
                                                    match String::from_utf8(message.body) {
                                                        Ok(payload_json) => payload_json,
                                                        Err(e) => String::from_utf8_lossy(
                                                            &e.into_bytes(),
                                                        )
                                                        .into_owned(),
                                                    };
                                                log::debug!("Got FCM Message: {:?}", payload_json);
                                                if let Some(id) = message.persistent_id.clone() {
                                                    let _ =
                                                        sender.try_send(PnRequest::AddPushID { id });
                                                }
                                                thread_pn_watcher.send_replace(Some(PushNoti {
                                                    message: payload_json,
                                                    id: message.persistent_id,
                                                }));
                                            }
                                            fcm_push_listener::Message::HeartbeatPing => {
                                                stream
                                                    .write_all(&fcm_push_listener::new_heartbeat_ack())
                                                    .await
                                                    .map_err(fcm_push_listener::Error::Socket)?;
                                            }
                                            fcm_push_listener::Message::Other(tag, bytes) => {
                                                log::trace!(
                                                    "Ignoring FCM message tag {} ({} bytes)",
                                                    tag,
                                                    bytes.len()
                                                );
                                            }
                                        }
                                    }
                                    AnyResult::Ok(())
                                })
                                .await;
                            match &r {
                                Ok(Ok(_)) => {
                                    log::debug!(
                                        "Push notification listener reported normal shutdown"
                                    );
                                }
                                Ok(Err(e)) => {
                                    match e.downcast_ref::<fcm_push_listener::Error>() {
                                        Some(
                                            fcm_push_listener::Error::MissingCryptoMetadata(_)
                                            | fcm_push_listener::Error::ProtobufDecode(_, _)
                                            | fcm_push_listener::Error::Base64Decode(_, _)
                                            | fcm_push_listener::Error::Crypto(_, _)
                                            | fcm_push_listener::Error::EmptyPayload,
                                        ) => {
                                            clear_registration(token_path.as_deref());
                                            log::debug!(
                                                "Error on push notification listener: {:?}. Clearing token",
                                                e
                                            );
                                        }
                                        Some(fcm_push_listener::Error::Request(_, e))
                                            if e.is_request()
                                                || e.is_connect()
                                                || e.is_timeout() =>
                                        {
                                            log::debug!(
                                                "Error on push notification listener: {:?}",
                                                e
                                            );
                                        }
                                        Some(fcm_push_listener::Error::Response(_, e))
                                            if e.is_request()
                                                || e.is_connect()
                                                || e.is_timeout() =>
                                        {
                                            log::debug!(
                                                "Error on push notification listener: {:?}",
                                                e
                                            );
                                        }
                                        _ => {
                                            log::debug!(
                                                "Error on push notification listener: {:?}",
                                                e
                                            );
                                            sleep(Duration::from_secs(30)).await;
                                        }
                                    }
                                }
                                Err(_) => {
                                    continue;
                                }
                            };
                            break;
                        }
                        AnyResult::Ok(())
                    }
                    .await;
                    v
                } => v,
                v = async {
                    while let Some(msg) = pn_request_rx.recv().await {
                        match msg {
                            PnRequest::Get{sender} => {
                                let _ = sender.send(self.pn_watcher.subscribe());
                            }
                            PnRequest::Activate{instance, sender} => {
                                let camera_uid = instance.uid().await?;
                                let uid = uid.clone();
                                let fcm_token = fcm_token.clone();
                                self.registed_cameras.insert(camera_uid, instance.clone());
                                tokio::task::spawn(async move {
                                    let r = instance.run_task(|camera| {
                                        let fcm_token = fcm_token.clone();
                                        let uid = uid.clone();
                                        Box::pin(async move {
                                            let r = camera.send_pushinfo_android(&fcm_token, &uid).await;
                                            log::debug!(
                                                "Registered {} for push notifications: {:?}",
                                                camera.uid().await?,
                                                r
                                            );
                                            r?;
                                            AnyResult::Ok(())
                                        })
                                    }).await;
                                    let _ = sender.send(r);
                                });
                            }
                            PnRequest::AddPushID{id} => {
                                log::trace!("Recived Push Notifcation of ID: {id}");
                                let mut received_ids = received_ids.write().await;
                                if received_ids.len() >= MAX_RECEIVED_IDS {
                                    received_ids.clear();
                                }
                                received_ids.insert(id);
                            }
                        }
                    }
                    AnyResult::Ok(())
                } => {
                    // These are critical errors
                    log::debug!("Push Notification thread ended {:?}", v);
                    break AnyResult::Ok(());
                },
            };
        }
    }
}

fn token_path() -> Option<PathBuf> {
    dirs::config_dir().map(|mut d| {
        fs::create_dir(&d)
            .map_or_else(
                |res| {
                    if let std::io::ErrorKind::AlreadyExists = res.kind() {
                        Ok(())
                    } else {
                        Err(res)
                    }
                },
                Ok,
            )
            .expect("Unable to create directory for push notification settings: {d:?}");
        d.push("neolink");
        fs::create_dir(&d)
            .map_or_else(
                |res| {
                    if let std::io::ErrorKind::AlreadyExists = res.kind() {
                        Ok(())
                    } else {
                        Err(res)
                    }
                },
                Ok,
            )
            .expect("Unable to create directory for push notification settings: {d:?}");
        d.push("./neolink_token.toml");
        d
    })
}

fn read_registration(
    token_path: &Path,
) -> Result<Result<fcm_push_listener::Registration, toml::de::Error>, std::io::Error> {
    fs::read_to_string(token_path).map(|v| toml::from_str::<fcm_push_listener::Registration>(&v))
}

fn save_registration(token_path: Option<&Path>, registration: &fcm_push_listener::Registration) {
    let new_token = toml::to_string(registration).with_context(|| "Unable to serialise fcm token");
    match (token_path, new_token) {
        (Some(token_path), Ok(new_token)) => {
            if let Err(e) = fs::write(token_path, &new_token) {
                log::warn!(
                    "Unable to save push notification details ({}) to {:#?} because of the error {:#?}",
                    new_token,
                    token_path,
                    e
                );
            }
        }
        (_, Err(e)) => {
            log::warn!("Unable to serialise push notification details: {:?}", e);
        }
        (None, _) => {}
    }
}

fn clear_registration(token_path: Option<&Path>) {
    if let Some(token_path) = token_path {
        if let Err(e) = fs::write(token_path, "") {
            log::debug!(
                "Unable to clear push notification token at {:?}: {:?}",
                token_path,
                e
            );
        }
    }
}

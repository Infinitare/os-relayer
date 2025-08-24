use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::{Arc, RwLock};
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, SystemTime};
use agave_banking_stage_ingress_types::BankingPacketBatch;
use crossbeam_channel::Sender;
use log::{error, warn};
use prost_types::Timestamp;
use solana_sdk::pubkey::Pubkey;
use tokio::sync::mpsc::{channel, error::TrySendError, Sender as TokioSender};
use tonic::{Request, Response, Status};
use tonic::codegen::tokio_stream::wrappers::ReceiverStream;
use crate::protos::{
    convert::packet_to_proto_packet,
    packet::PacketBatch as ProtoPacketBatch,
    relayer::SubscribePacketsResponse
};
use crate::protos::relayer::relayer_server::Relayer;
use crate::protos::relayer::{subscribe_packets_response, GetTpuConfigsRequest, GetTpuConfigsResponse, SubscribePacketsRequest};
use crate::protos::shared::{Header, Heartbeat, Socket};
use crate::relayer::tpu::Tpu;

type PacketSubscriptions = Arc<RwLock<HashMap<Pubkey, TokioSender<Result<SubscribePacketsResponse, Status>>>>>;

pub struct Forwarder {
    public_ip: IpAddr,
    tpu_quic_port: u16,
    tpu_fwd_quic_port: u16,
    packet_subscriptions: PacketSubscriptions,
}

impl Forwarder {
    pub const SUBSCRIBER_QUEUE_CAPACITY: usize = 50_000;
    pub const VALIDATOR_PACKET_BATCH_SIZE: usize = 4;

    pub fn new(
        public_ip: &IpAddr,
        tpu_quic_port: u16,
        tpu_fwd_quic_port: u16,
        exit: &Arc<AtomicBool>,
    ) -> (Self, Vec<thread::JoinHandle<()>>, Sender<BankingPacketBatch>) {
        let (delay_packet_sender, delay_packet_receiver) = crossbeam_channel::bounded(Tpu::TPU_QUEUE_CAPACITY);
        let packet_subscriptions = Arc::new(RwLock::new(HashMap::default()));

        let thread = {
            let packet_subscriptions = packet_subscriptions.clone();
            let exit = exit.clone();
            thread::Builder::new()
                .name("forwarder-event_loop_thread".to_string())
                .spawn(move || {
                    let res = Self::run_event_loop(
                        delay_packet_receiver,
                        packet_subscriptions,
                        &exit,
                    );
                    if !exit.load(Ordering::Relaxed) && let Err(err) = res {
                        error!("RelayerImpl thread exited with result {err}")
                    }
                })
                .unwrap()
        };

        (
            Self {
                public_ip: public_ip.clone(),
                tpu_quic_port,
                tpu_fwd_quic_port,
                packet_subscriptions,
            },
            vec![thread],
            delay_packet_sender,
        )
    }

    fn run_event_loop(
        delay_packet_receiver: crossbeam_channel::Receiver<BankingPacketBatch>,
        packet_subscriptions: PacketSubscriptions,
        exit: &Arc<AtomicBool>,
    ) -> Result<(), String>  {
        let heartbeat_tick = crossbeam_channel::tick(Duration::from_millis(100));
        let mut heartbeat_count = 0;

        while !exit.load(Ordering::Relaxed) {
            crossbeam_channel::select! {
                recv(delay_packet_receiver) -> maybe_packet_batches => {
                    let failed_forwards = Self::forward_packets(&packet_subscriptions, maybe_packet_batches)?;
                    Self::drop_connections(&packet_subscriptions, failed_forwards);
                }
                recv(heartbeat_tick) -> _ => {
                    let failed_forwards = Self::handle_heartbeat(&packet_subscriptions, &mut heartbeat_count)?;
                    Self::drop_connections(&packet_subscriptions, failed_forwards);
                }
            }
        }

        Ok(())
    }

    fn add_subscription(
        &self,
        identity: Pubkey,
        sender: TokioSender<Result<SubscribePacketsResponse, Status>>,
    ) -> Result<(), String> {
        let mut l_subscriptions = self.packet_subscriptions.write().unwrap();
        let len = l_subscriptions.len();

        match l_subscriptions.entry(identity) {
            Entry::Vacant(entry) => {
                if len >= 1 {
                    return Err(format!(
                        "too many subscriptions, max is 1, currently connected: {:?}",
                        l_subscriptions
                    ));
                }

                entry.insert(sender);
            }
            Entry::Occupied(mut entry) => {
                error!("already connected, dropping old connection: {identity:?}");
                entry.insert(sender);
            }
        }

        Ok(())
    }

    fn drop_connections(
        subscriptions: &PacketSubscriptions,
        failed_forwards: Vec<Pubkey>,
    ) {
        let mut l_subscriptions = subscriptions.write().unwrap();
        for disconnected in failed_forwards {
            if let Some(sender) = l_subscriptions.remove(&disconnected) {
                drop(sender);
            }
        }
    }

    fn forward_packets(
        packet_subscriptions: &PacketSubscriptions,
        maybe_packet_batches: Result<BankingPacketBatch, crossbeam_channel::RecvError>,
    ) -> Result<Vec<Pubkey>, String> {
        let packets: Vec<_> = maybe_packet_batches
            .map_err(|err| err.to_string())?
            .iter()
            .flat_map(|batch| {
                batch
                    .iter()
                    .filter(|p| !p.meta().discard())
                    .filter_map(packet_to_proto_packet)
            })
            .collect();

        let mut proto_packet_batches =
            Vec::with_capacity((packets.len() / Forwarder::VALIDATOR_PACKET_BATCH_SIZE) + 1);
        for packet_chunk in packets.chunks(Forwarder::VALIDATOR_PACKET_BATCH_SIZE) {
            proto_packet_batches.push(ProtoPacketBatch {
                packets: packet_chunk.to_vec(),
            });
        }

        let l_subscriptions = packet_subscriptions.read().unwrap();
        let mut failed_forwards = Vec::new();
        for batch in &proto_packet_batches {
            if batch.packets.is_empty() {
                continue;
            }

            for (pubkey, sender) in l_subscriptions.iter() {
                match sender.try_send(Ok(SubscribePacketsResponse {
                    header: Some(Header {
                        ts: Some(Timestamp::from(SystemTime::now())),
                    }),
                    msg: Some(subscribe_packets_response::Msg::Batch(batch.clone())),
                })) {
                    Ok(_) => {}
                    Err(TrySendError::Full(_)) => {
                        error!("packet channel is full for pubkey: {:?}", pubkey);
                    }
                    Err(TrySendError::Closed(_)) => {
                        error!("channel is closed for pubkey: {:?}", pubkey);
                        failed_forwards.push(pubkey.clone());
                        break;
                    }
                }
            }
        }

        Ok(failed_forwards)
    }

    fn handle_heartbeat(
        packet_subscriptions: &PacketSubscriptions,
        heartbeat_count: &mut u64,
    ) -> Result<Vec<Pubkey>, String> {
        let failed_pubkey_updates = packet_subscriptions
            .read()
            .unwrap()
            .iter()
            .filter_map(|(pubkey, sender)| {
                match sender.try_send(Ok(SubscribePacketsResponse {
                    header: None,
                    msg: Some(subscribe_packets_response::Msg::Heartbeat(Heartbeat {
                        count: heartbeat_count.clone(),
                    })),
                })) {
                    Ok(_) => {}
                    Err(TrySendError::Closed(_)) => return Some(*pubkey),
                    Err(TrySendError::Full(_)) => {
                        warn!("heartbeat channel is full for: {:?}", pubkey);
                    }
                }
                None
            })
            .collect();

        *heartbeat_count += 1;
        Ok(failed_pubkey_updates)
    }
}

#[tonic::async_trait]
impl Relayer for Forwarder {
    async fn get_tpu_configs(
        &self,
        _: Request<GetTpuConfigsRequest>,
    ) -> Result<Response<GetTpuConfigsResponse>, Status> {
        Ok(Response::new(GetTpuConfigsResponse {
            tpu: Some(Socket {
                ip: self.public_ip.to_string(),
                port: (self.tpu_quic_port - 6) as i64,
            }),
            tpu_forward: Some(Socket {
                ip: self.public_ip.to_string(),
                port: (self.tpu_fwd_quic_port - 6) as i64,
            }),
        }))
    }

    type SubscribePacketsStream = ReceiverStream<Result<SubscribePacketsResponse, Status>>;

    async fn subscribe_packets(
        &self,
        request: Request<SubscribePacketsRequest>,
    ) -> Result<Response<Self::SubscribePacketsStream>, Status> {
        let pubkey: &Pubkey = request
            .extensions()
            .get()
            .ok_or_else(|| Status::internal("internal error fetching public key"))?;

        let (sender, receiver) = channel(Forwarder::SUBSCRIBER_QUEUE_CAPACITY);
        self.add_subscription(pubkey.clone(), sender)
            .map_err(|err| Status::internal(err))?;

        Ok(Response::new(ReceiverStream::new(receiver)))
    }
}

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use agave_banking_stage_ingress_types::BankingPacketBatch;
use futures_util::{StreamExt};
use log::{debug, error, info};
use solana_perf::packet::{PacketBatch, PinnedPacketBatch};
use tokio::sync::{broadcast, mpsc};
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Streaming};
use tonic::transport::{Channel, Endpoint};
use crate::protos::block_engine::{SubscribeBundlesResponse, SubscribePacketsResponse};
use crate::protos::convert::{packet_to_proto_packet, proto_packet_to_packet};
use crate::protos::{inf, packet};
use crate::protos::inf::inf_service_client::InfServiceClient;

#[derive(Clone)]
pub struct Client {
    signed_message: Vec<u8>,
    endpoint: Endpoint,
    connected: Arc<AtomicBool>,
    exit: Arc<AtomicBool>,
}

impl Client {
    pub fn new(
        signed_message: Vec<u8>,
        proxy: Endpoint,
        exit: &Arc<AtomicBool>,
    ) -> Self {
        Client {
            signed_message,
            endpoint: proxy,
            connected: Arc::new(AtomicBool::new(false)),
            exit: exit.clone(),
        }
    }

    pub fn start_connection_thread(
        &self,
        rt: &tokio::runtime::Handle,
        packet_sender: crossbeam_channel::Sender<BankingPacketBatch>,
        jito_bundle_sender: broadcast::Sender<SubscribeBundlesResponse>,
        jito_packets_sender: broadcast::Sender<SubscribePacketsResponse>,
        internal_proxy_packet_receiver: crossbeam_channel::Receiver<BankingPacketBatch>,
        internal_proxy_jito_bundle_receiver: crossbeam_channel::Receiver<SubscribeBundlesResponse>,
        internal_proxy_jito_packets_receiver: crossbeam_channel::Receiver<SubscribePacketsResponse>,
    ) -> JoinHandle<()> {
        let self_clone = self.clone();
        let rt_clone = rt.clone();
        rt.spawn(async move {
            while !self_clone.exit.load(Ordering::Relaxed) {
                let channel = match self_clone.endpoint.connect().await {
                    Ok(channel) => channel,
                    Err(e) => {
                        error!("Failed to connect to proxy: {}", e);
                        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                        continue;
                    }
                };

                let client_instance = InfServiceClient::new(channel);

                if let Err(err) = self_clone.start_subscribing(
                    client_instance,
                    &rt_clone,
                    &packet_sender,
                    &jito_bundle_sender,
                    &jito_packets_sender,
                    &internal_proxy_packet_receiver,
                    &internal_proxy_jito_bundle_receiver,
                    &internal_proxy_jito_packets_receiver,
                ).await {
                    error!("Failed to start subscribing: {}", err);
                }

                self_clone.connected.store(false, Ordering::Relaxed);
                if !self_clone.exit.load(Ordering::Relaxed) {
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await
                }
            }
        })
    }

    async fn start_subscribing(
        &self,
        mut client: InfServiceClient<Channel>,
        rt: &tokio::runtime::Handle,
        packet_sender: &crossbeam_channel::Sender<BankingPacketBatch>,
        jito_bundle_sender: &broadcast::Sender<SubscribeBundlesResponse>,
        jito_packets_sender: &broadcast::Sender<SubscribePacketsResponse>,
        internal_proxy_packet_receiver: &crossbeam_channel::Receiver<BankingPacketBatch>,
        internal_proxy_jito_bundle_receiver: &crossbeam_channel::Receiver<SubscribeBundlesResponse>,
        internal_proxy_jito_packets_receiver: &crossbeam_channel::Receiver<SubscribePacketsResponse>,
    ) -> Result<(), String> {
        let header = inf::Header {
            signed_message: self.signed_message.clone(),
        };

        let packet_request = inf::SubscribePacketsRequest { header: Some(header.clone()) };
        let bundles_request = inf::SubscribeBundlesRequest { header: Some(header.clone()) };

        let packet_stream = client.subscribe_packets(packet_request.clone()).await.map_err(|e| e.to_string())?.into_inner();
        let jito_bundles_stream = client.subscribe_jito_bundles(bundles_request).await.map_err(|e| e.to_string())?.into_inner();
        let jito_packets_stream = client.subscribe_jito_packets(packet_request).await.map_err(|e| e.to_string())?.into_inner();

        self.connected.store(true, Ordering::Relaxed);
        info!("Connected to proxy");

        let subscription_join = self.handle_subscriptions(
            rt,
            packet_stream,
            packet_sender.clone(),
            jito_bundles_stream,
            jito_bundle_sender.clone(),
            jito_packets_stream,
            jito_packets_sender.clone()
        );

        let streaming_join = self.handle_streaming(
            rt,
            client,
            header,
            internal_proxy_packet_receiver,
            internal_proxy_jito_bundle_receiver,
            internal_proxy_jito_packets_receiver,
        );

        streaming_join.await.map_err(|e| e.to_string())?;
        subscription_join.await.map_err(|e| e.to_string())?;
        Ok(())
    }

    fn handle_subscriptions(
        &self,
        rt: &tokio::runtime::Handle,
        packet_stream: Streaming<SubscribePacketsResponse>,
        packet_sender: crossbeam_channel::Sender<BankingPacketBatch>,
        jito_bundles_stream: Streaming<SubscribeBundlesResponse>,
        jito_bundle_sender: broadcast::Sender<SubscribeBundlesResponse>,
        jito_packets_stream: Streaming<SubscribePacketsResponse>,
        jito_packets_sender: broadcast::Sender<SubscribePacketsResponse>,
    ) -> JoinHandle<()> {
        let exit = self.exit.clone();
        let packets_join = Self::subscription_thread(
            rt.clone(),
            exit.clone(),
            packet_stream,
            move |packet| {
                let batch = match packet.batch {
                    Some(packets) => packets,
                    None => return Ok(()),
                };

                let bytes_packet_batch = PinnedPacketBatch::new(
                    batch.packets.iter().map(proto_packet_to_packet).collect()
                );

                let banking_packet_batch: BankingPacketBatch = Arc::new(vec![PacketBatch::from(bytes_packet_batch)]);
                packet_sender.send(banking_packet_batch).map_err(|e| format!("Failed to send packet batch: {}", e))
            }
        );

        let jito_bundles_join = Self::subscription_thread(
            rt.clone(),
            exit.clone(),
            jito_bundles_stream,
            move |bundle| {
                jito_bundle_sender.send(bundle).map_err(|e| format!("Failed to send jito bundle: {}", e))?;
                Ok(())
            }
        );

        let jito_packets_join = Self::subscription_thread(
            rt.clone(),
            exit.clone(),
            jito_packets_stream,
            move |packet| {
                jito_packets_sender.send(packet).map_err(|e| format!("Failed to send jito packet: {}", e))?;
                Ok(())
            }
        );

        rt.spawn(async move {
            let _ = futures_util::future::join3(packets_join, jito_bundles_join, jito_packets_join).await;
        })
    }

    async fn subscription_thread<T, F>(
        rt: tokio::runtime::Handle,
        exit: Arc<AtomicBool>,
        mut stream: Streaming<T>,
        sender: F,
    ) -> JoinHandle<()>
    where
        T: Send + 'static + std::fmt::Debug,
        F: Fn(T) -> Result<(), String> + Send + 'static,
    {
        rt.spawn(async move {
            let mut tick = tokio::time::interval(tokio::time::Duration::from_millis(100));
            loop {
                tokio::select! {
                    _ = tick.tick() => {
                        if exit.load(Ordering::Relaxed) {
                            break;
                        }
                    },
                    packet = stream.next() => {
                        match packet {
                            Some(packet) => {
                                match packet {
                                    Ok(packet) => {
                                        if let Err(err) = sender(packet) {
                                            error!("Failed to send message: {}", err);
                                            break;
                                        }
                                    },
                                    Err(err) => {
                                        if !exit.load(Ordering::Relaxed) {
                                            error!("Error receiving message: {}", err);
                                        }
                                        break;
                                    }
                                }
                            },
                            None => {
                                info!("Stream ended");
                                break;
                            }
                        }
                    }
                }
            }
        })
    }

    fn handle_streaming(
        &self,
        rt: &tokio::runtime::Handle,
        client: InfServiceClient<Channel>,
        header: inf::Header,
        internal_proxy_packet_receiver: &crossbeam_channel::Receiver<BankingPacketBatch>,
        internal_proxy_jito_bundle_receiver: &crossbeam_channel::Receiver<SubscribeBundlesResponse>,
        internal_proxy_jito_packets_receiver: &crossbeam_channel::Receiver<SubscribePacketsResponse>,
    ) -> JoinHandle<()> {
        let packet_stream = Self::stream_packets(
            rt.clone(),
            client.clone(),
            header.clone(),
            internal_proxy_packet_receiver.clone()
        );

        let jito_bundle_stream = Self::stream_jito_bundles(
            rt.clone(),
            client.clone(),
            header.clone(),
            internal_proxy_jito_bundle_receiver.clone()
        );

        let jito_packets_stream = Self::stream_jito_packets(
            rt.clone(),
            client,
            header,
            internal_proxy_jito_packets_receiver.clone()
        );

        rt.spawn(async move {
            let _ = futures_util::future::join3(packet_stream, jito_bundle_stream, jito_packets_stream).await;
        })
    }

    fn stream_packets(
        rt: tokio::runtime::Handle,
        mut client: InfServiceClient<Channel>,
        header: inf::Header,
        internal_proxy_packet_receiver: crossbeam_channel::Receiver<BankingPacketBatch>,
    ) -> JoinHandle<()> {
        rt.spawn(async move {
            let (tx, rx_mpsc) = mpsc::channel::<inf::StreamPacketsRequest>(256);
            let rpc = tokio::spawn(async move {
                let stream = ReceiverStream::new(rx_mpsc);
                client.stream_packets(Request::new(stream)).await
            });

            let _ = tx.send(inf::StreamPacketsRequest {
                kind: Some(inf::stream_packets_request::Kind::Header(header)),
            }).await;

            while let Ok(pkt) = internal_proxy_packet_receiver.recv() {
                for batch in pkt.iter() {
                    let packet_batch = packet::PacketBatch {
                        packets: batch.iter().filter_map(packet_to_proto_packet).collect(),
                    };

                    if let Err(err) = tx.send(inf::StreamPacketsRequest {
                        kind: Some(inf::stream_packets_request::Kind::Batch(packet_batch)),
                    }).await {
                        error!("Failed to send packet batch to stream {}", err);
                        break;
                    }
                }
            }

            drop(tx);
            if let Err(err) = rpc.await {
                error!("RPC stream failed: {}", err);
            }
        })
    }

    fn stream_jito_bundles(
        rt: tokio::runtime::Handle,
        mut client: InfServiceClient<Channel>,
        header: inf::Header,
        internal_proxy_packet_receiver: crossbeam_channel::Receiver<SubscribeBundlesResponse>,
    ) -> JoinHandle<()> {
        rt.spawn(async move {
            let (tx, rx_mpsc) = mpsc::channel::<inf::StreamBundlesRequest>(256);
            let rpc = tokio::spawn(async move {
                let stream = ReceiverStream::new(rx_mpsc);
                client.stream_jito_bundles(Request::new(stream)).await
            });

            let _ = tx.send(inf::StreamBundlesRequest {
                kind: Some(inf::stream_bundles_request::Kind::Header(header)),
            }).await;

            while let Ok(pkt) = internal_proxy_packet_receiver.recv() {
                if let Err(err) = tx.send(inf::StreamBundlesRequest {
                    kind: Some(inf::stream_bundles_request::Kind::Bundle(pkt)),
                }).await {
                    error!("Failed to send packet batch to stream {}", err);
                    break;
                }
            }

            drop(tx);
            if let Err(err) = rpc.await {
                error!("RPC stream failed: {}", err);
            }
        })
    }

    fn stream_jito_packets(
        rt: tokio::runtime::Handle,
        mut client: InfServiceClient<Channel>,
        header: inf::Header,
        internal_proxy_packet_receiver: crossbeam_channel::Receiver<SubscribePacketsResponse>,
    ) -> JoinHandle<()> {
        rt.spawn(async move {
            let (tx, rx_mpsc) = mpsc::channel::<inf::StreamPacketsRequest>(256);
            let rpc = tokio::spawn(async move {
                let stream = ReceiverStream::new(rx_mpsc);
                client.stream_jito_packets(Request::new(stream)).await
            });

            let _ = tx.send(inf::StreamPacketsRequest {
                kind: Some(inf::stream_packets_request::Kind::Header(header)),
            }).await;

            while let Ok(pkt) = internal_proxy_packet_receiver.recv() {
                if pkt.batch.is_none() {
                    continue;
                }

                if let Err(err) = tx.send(inf::StreamPacketsRequest {
                    kind: Some(inf::stream_packets_request::Kind::Batch(pkt.batch.unwrap())),
                }).await {
                    error!("Failed to send packet batch to stream {}", err);
                    break;
                }
            }

            drop(tx);
            if let Err(err) = rpc.await {
                error!("RPC stream failed: {}", err);
            }
        })
    }

    pub fn handle_routing(
        &self,
        rt: &tokio::runtime::Handle,
        packet_receiver: crossbeam_channel::Receiver<BankingPacketBatch>,
        jito_bundle_receiver: crossbeam_channel::Receiver<SubscribeBundlesResponse>,
        jito_packets_receiver: crossbeam_channel::Receiver<SubscribePacketsResponse>,
        packet_sender: crossbeam_channel::Sender<BankingPacketBatch>,
        jito_bundle_sender: broadcast::Sender<SubscribeBundlesResponse>,
        jito_packets_sender: broadcast::Sender<SubscribePacketsResponse>,
        internal_proxy_packet_sender: crossbeam_channel::Sender<BankingPacketBatch>,
        internal_proxy_jito_bundle_sender: crossbeam_channel::Sender<SubscribeBundlesResponse>,
        internal_proxy_jito_packets_sender: crossbeam_channel::Sender<SubscribePacketsResponse>,
    ) -> JoinHandle<()> {
        let self_clone = self.clone();
        let rt_clone = rt.clone();
        rt.spawn(async move {
            let direct_redirect = Arc::new(AtomicBool::new(true));

            let exit = self_clone.exit.clone();
            let direct_redirect_clone = direct_redirect.clone();

            let handler_join = rt_clone.spawn(async move {
                while !self_clone.exit.load(Ordering::Relaxed) {
                    direct_redirect_clone.store(
                        !self_clone.connected.load(Ordering::Relaxed),
                        Ordering::Relaxed
                    );

                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                }
            });

            let packets_join = Self::route_thread(
                &rt_clone,
                &exit,
                &direct_redirect,
                packet_receiver,
                move |packet, cached_direct_redirect| {
                    debug!("Received packet - direct_redirect: {}", cached_direct_redirect);
                    if cached_direct_redirect {
                        packet_sender
                            .send(packet)
                            .map_err(|e| format!("Failed to send packet to sender: {}", e))
                    } else {
                        internal_proxy_packet_sender
                            .send(packet)
                            .map_err(|e| format!("Failed to send packet to internal proxy: {}", e))
                    }
                }
            );

            let jito_bundles_join = Self::route_thread(
                &rt_clone,
                &exit,
                &direct_redirect,
                jito_bundle_receiver,
                move |packet, cached_direct_redirect| {
                    debug!("Received jito bundle - direct_redirect: {}", cached_direct_redirect);
                    if cached_direct_redirect {
                        jito_bundle_sender
                            .send(packet)
                            .map_err(|e| format!("Failed to send jito bundle to sender: {}", e))?;
                        Ok(())
                    } else {
                        internal_proxy_jito_bundle_sender
                            .send(packet)
                            .map_err(|e| format!("Failed to send jito bundle to internal proxy: {}", e))
                    }
                }
            );

            let jito_packets_join = Self::route_thread(
                &rt_clone,
                &exit,
                &direct_redirect,
                jito_packets_receiver,
                move |packet, cached_direct_redirect| {
                    debug!("Received jito packet - direct_redirect: {}", cached_direct_redirect);
                    if cached_direct_redirect {
                        jito_packets_sender
                            .send(packet)
                            .map_err(|e| format!("Failed to send jito packets to sender: {}", e))?;
                        Ok(())
                    } else {
                        internal_proxy_jito_packets_sender
                            .send(packet)
                            .map_err(|e| format!("Failed to send jito packets to internal proxy: {}", e))
                    }
                }
            );

            let _ = futures_util::future::join4(
                handler_join,
                packets_join,
                jito_bundles_join,
                jito_packets_join
            ).await;
        })
    }

    fn route_thread<T, F>(
        rt: &tokio::runtime::Handle,
        exit: &Arc<AtomicBool>,
        direct_redirect: &Arc<AtomicBool>,
        receiver: crossbeam_channel::Receiver<T>,
        send: F
    ) -> JoinHandle<()>
    where
        T: Send + 'static + std::fmt::Debug,
        F: Fn(T, bool) -> Result<(), String> + Send + 'static,
    {
        let exit_clone = exit.clone();
        let direct_redirect_clone = direct_redirect.clone();
        rt.spawn(async move {
            let tick = crossbeam_channel::tick(std::time::Duration::from_millis(100));
            let mut cached_direct_redirect = true;
            loop {
                crossbeam_channel::select! {
                    recv(tick) -> _ => {
                        if exit_clone.load(Ordering::Relaxed) {
                            break;
                        }

                        if direct_redirect_clone.load(Ordering::Relaxed) {
                            cached_direct_redirect = true;
                        } else {
                            cached_direct_redirect = false;
                        }
                    },
                    recv(receiver) -> res => {
                        let current_timestamp_millis = chrono::Utc::now().timestamp_millis();
                        match res {
                            Ok(packet) => {
                                if let Err(err) = send(packet, cached_direct_redirect) {
                                    error!("Failed to redirect message: {}", err);
                                    break;
                                }
                                let elapsed = chrono::Utc::now().timestamp_millis() - current_timestamp_millis;
                                debug!("Message redirected in {}ms - direct_redirect: {}", elapsed, cached_direct_redirect);
                            }
                            Err(err) => {
                                if !exit_clone.load(Ordering::Relaxed) {
                                    error!("Failed to receive message: {}", err);
                                }
                                break;
                            }
                        }
                    }
                }
            }

            exit_clone.store(true, Ordering::Relaxed);
        })
    }
}

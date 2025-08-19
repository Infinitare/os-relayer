mod client;

use std::str::FromStr;
use std::sync::{Arc};
use std::sync::atomic::{AtomicBool};
use agave_banking_stage_ingress_types::BankingPacketBatch;
use log::{info};
use solana_sdk::signer::Signer;
use tokio::sync::{broadcast};
use tokio::task::JoinHandle;
use tonic::transport::{Endpoint};
use crate::protos::block_engine::{SubscribeBundlesResponse, SubscribePacketsResponse};
use crate::proxy::client::Client;

pub struct Proxy {
    connection: JoinHandle<()>,
    routing: JoinHandle<()>,
}

impl Proxy {
    const PROXY_CHANNEL_LIMIT: usize = 50_000;

    pub fn new(
        keypair: &solana_sdk::signature::Keypair,
        rt: &tokio::runtime::Handle,
        proxy: &String,
        packet_sender: crossbeam_channel::Sender<BankingPacketBatch>,
        packet_receiver: crossbeam_channel::Receiver<BankingPacketBatch>,
        jito_bundle_sender: broadcast::Sender<SubscribeBundlesResponse>,
        jito_bundle_receiver: crossbeam_channel::Receiver<SubscribeBundlesResponse>,
        jito_packets_sender: broadcast::Sender<SubscribePacketsResponse>,
        jito_packets_receiver: crossbeam_channel::Receiver<SubscribePacketsResponse>,
        sign_message: &String,
        exit: &Arc<AtomicBool>,
    ) -> Self {
        let signed = Signer::sign_message(
            keypair,
            sign_message.as_bytes(),
        );

        info!("Connecting Proxy to {}", proxy);
        let client = Client::new(
            signed.to_string().as_bytes().to_vec(),
            Endpoint::from_str(proxy).expect("valid proxy url"),
            &exit,
        );

        let (internal_proxy_packet_sender, internal_proxy_packet_receiver) = crossbeam_channel::bounded(Self::PROXY_CHANNEL_LIMIT);
        let (internal_proxy_jito_bundle_sender, internal_proxy_jito_bundle_receiver) = crossbeam_channel::bounded(Self::PROXY_CHANNEL_LIMIT);
        let (internal_proxy_jito_packets_sender, internal_proxy_jito_packets_receiver) = crossbeam_channel::bounded(Self::PROXY_CHANNEL_LIMIT);

        let connection_join = client.start_connection_thread(
            rt,
            packet_sender.clone(),
            jito_bundle_sender.clone(),
            jito_packets_sender.clone(),
            internal_proxy_packet_receiver,
            internal_proxy_jito_bundle_receiver,
            internal_proxy_jito_packets_receiver,
        );

        let routing_join = client.handle_routing(
            rt,
            packet_receiver,
            jito_bundle_receiver,
            jito_packets_receiver,
            packet_sender,
            jito_bundle_sender,
            jito_packets_sender,
            internal_proxy_packet_sender,
            internal_proxy_jito_bundle_sender,
            internal_proxy_jito_packets_sender,
        );

        Proxy {
            connection: connection_join,
            routing: routing_join,
        }
    }

    pub async fn join(self) {
        self.connection.await.unwrap();
        self.routing.await.unwrap();
    }
}

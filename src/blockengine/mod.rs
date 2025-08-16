mod grpc_server;

use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::thread;
use log::info;
use tokio::sync::broadcast;
use tonic::transport::Server;
use crate::blockengine::grpc_server::GrpcServer;
use crate::helper::shutdown_signal;
use crate::protos::auth::auth_service_server::AuthServiceServer;
use crate::protos::block_engine::{SubscribeBundlesResponse, SubscribePacketsResponse};
use crate::protos::block_engine::block_engine_validator_server::BlockEngineValidatorServer;

pub struct Blockengine {
    server: tokio::task::JoinHandle<()>,
}

impl Blockengine {
    const BLOCKENGINE_CHANNEL_LIMIT: usize = 50_000;

    pub fn new(
        rt: &tokio::runtime::Handle,
        grpc_bind_ip: &IpAddr,
        blockengine_bind_port: u16,
        jito_blockengine: String,
        exit: &Arc<AtomicBool>,
    ) -> (Self, broadcast::Sender<SubscribeBundlesResponse>, crossbeam_channel::Receiver<SubscribeBundlesResponse>, broadcast::Sender<SubscribePacketsResponse>, crossbeam_channel::Receiver<SubscribePacketsResponse>) {
        let (bundles_sender_from_proxy, _) = broadcast::channel::<SubscribeBundlesResponse>(Self::BLOCKENGINE_CHANNEL_LIMIT);
        let (packets_sender_from_proxy, _) = broadcast::channel::<SubscribePacketsResponse>(Self::BLOCKENGINE_CHANNEL_LIMIT);

        let (bundles_sender_from_blockengine, bundles_receiver_from_blockengine) = crossbeam_channel::bounded(Self::BLOCKENGINE_CHANNEL_LIMIT);
        let (packets_sender_from_blockengine, packets_receiver_from_blockengine) = crossbeam_channel::bounded(Self::BLOCKENGINE_CHANNEL_LIMIT);

        let server = GrpcServer::new(
            rt,
            jito_blockengine,
            bundles_sender_from_proxy.clone(),
            bundles_sender_from_blockengine.clone(),
            packets_sender_from_proxy.clone(),
            packets_sender_from_blockengine.clone(),
        );

        let server_addr = SocketAddr::new(grpc_bind_ip.clone(), blockengine_bind_port);
        let exit = exit.clone();
        info!("starting blockengine at: {:?}", server_addr);

        let server = rt.spawn(async move {
            Server::builder()
                .add_service(BlockEngineValidatorServer::new(server.clone()))
                .add_service(AuthServiceServer::new(server))
                .serve_with_shutdown(server_addr, shutdown_signal(exit.clone()))
                .await
                .expect("serve relayer");
        });

        (
            Blockengine {
                server,
            },
            bundles_sender_from_proxy,
            bundles_receiver_from_blockengine,
            packets_sender_from_proxy,
            packets_receiver_from_blockengine,
        )
    }

    pub async fn join(self) -> thread::Result<()> {
        self.server.await.expect("blockengine server join");
        Ok(())
    }
}

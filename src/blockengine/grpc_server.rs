use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use dashmap::DashMap;
use futures_util::{future, StreamExt, TryStreamExt};
use log::{error, info, warn};
use tokio::sync::broadcast;
use tonic::codegen::BoxStream;
use tonic::{Code, Request, Response, Status};
use tonic::transport::{Channel, Endpoint};
use tokio_stream::{Stream, wrappers::{BroadcastStream}};
use crate::protos::auth::auth_service_server::AuthService;
use crate::protos::auth::{GenerateAuthChallengeRequest, GenerateAuthChallengeResponse, GenerateAuthTokensRequest, GenerateAuthTokensResponse, RefreshAccessTokenRequest, RefreshAccessTokenResponse};
use crate::protos::auth::auth_service_client::AuthServiceClient;
use crate::protos::block_engine::block_engine_validator_server::BlockEngineValidator;
use crate::protos::block_engine::{BlockBuilderFeeInfoRequest, BlockBuilderFeeInfoResponse, SubscribeBundlesRequest, SubscribeBundlesResponse, SubscribePacketsRequest, SubscribePacketsResponse};
use crate::protos::block_engine::block_engine_validator_client::BlockEngineValidatorClient;

#[derive(Clone)]
pub struct GrpcServer {
    block_engine_url: String,
    rt: tokio::runtime::Handle,

    client_pool: Arc<DashMap<IpAddr, BlockEngineValidatorClient<Channel>>>,
    auth_pool: Arc<DashMap<IpAddr, AuthServiceClient<Channel>>>,

    bundles_sender_from_proxy: broadcast::Sender<SubscribeBundlesResponse>,
    bundles_sender_from_blockengine: crossbeam_channel::Sender<SubscribeBundlesResponse>,
    packets_sender_from_proxy: broadcast::Sender<SubscribePacketsResponse>,
    packets_sender_from_blockengine: crossbeam_channel::Sender<SubscribePacketsResponse>,

    exit: Arc<AtomicBool>,
}

impl GrpcServer {
    pub fn new(
        rt: &tokio::runtime::Handle,
        block_engine_url: String,
        bundles_sender_from_proxy: broadcast::Sender<SubscribeBundlesResponse>,
        bundles_sender_from_blockengine: crossbeam_channel::Sender<SubscribeBundlesResponse>,
        packets_sender_from_proxy: broadcast::Sender<SubscribePacketsResponse>,
        packets_sender_from_blockengine: crossbeam_channel::Sender<SubscribePacketsResponse>,
        exit: &Arc<AtomicBool>,
    ) -> Self {
        let client_pool = Arc::new(DashMap::new());
        let auth_pool = Arc::new(DashMap::new());

        GrpcServer {
            block_engine_url,
            rt: rt.clone(),
            client_pool,
            auth_pool,
            bundles_sender_from_proxy,
            bundles_sender_from_blockengine,
            packets_sender_from_proxy,
            packets_sender_from_blockengine,
            exit: exit.clone(),
        }
    }

    async fn get_block_engine_client(
        &self,
        peer: Option<SocketAddr>,
    ) -> Result<BlockEngineValidatorClient<Channel>, Status> {
        if let Some(addr) = peer {
            if let Some(existing) = self.client_pool.get(&addr.ip()) {
                return Ok(existing.clone());
            }
        }

        let channel = self.get_client().await?;

        let client = BlockEngineValidatorClient::new(channel);
        if let Some(addr) = peer {
            info!("adding client to block engine pool: {}", addr);
            self.client_pool.insert(addr.ip(), client.clone());
        }

        Ok(client)
    }

    async fn get_auth_client(
        &self,
        peer: Option<SocketAddr>,
    ) -> Result<AuthServiceClient<Channel>, Status> {
        if let Some(addr) = peer {
            if let Some(existing) = self.auth_pool.get(&addr.ip()) {
                return Ok(existing.clone());
            }
        }

        let channel = self.get_client().await?;

        let client = AuthServiceClient::new(channel);
        if let Some(addr) = peer {
            info!("adding client to auth service pool: {}", addr);
            self.auth_pool.insert(addr.ip(), client.clone());
        }

        Ok(client)
    }

    async fn get_client(
        &self,
    ) -> Result<Channel, Status> {
        let channel = Endpoint::from_shared(self.block_engine_url.clone())
            .map_err(|e| Status::internal(e.to_string()))?
            .connect()
            .await
            .map_err(|e| Status::unavailable(e.to_string()))?;

        Ok(channel)
    }

    fn make_response<T>(
        stream: impl Stream<Item = Result<T, Status>> + Send + 'static,
    ) -> Response<BoxStream<T>>
    where
        T: Send + 'static,
    {
        Response::new(Box::pin(stream) as BoxStream<T>)
    }
}

fn forwarder<T>(
    mut upstream: tonic::Streaming<T>,
    forward_sender: crossbeam_channel::Sender<T>,
    rt: tokio::runtime::Handle,
    exit: Arc<AtomicBool>,
)
where
    T: Clone + Send + 'static + std::fmt::Debug,
{
    rt.spawn(async move {
        let mut maintenance_tick = tokio::time::interval(std::time::Duration::from_millis(100));
        maintenance_tick.tick().await;

        loop {
            tokio::select! {
                _ = maintenance_tick.tick() => {
                    info!("maintenance tick {}", exit.load(std::sync::atomic::Ordering::Relaxed));
                    if exit.load(std::sync::atomic::Ordering::Relaxed) {
                        info!("Exiting forwarder task due to shutdown signal.");
                        return;
                    }
                }
                res = upstream.next() => {
                    if let Some(item) = res {
                        match item {
                            Ok(packet) => {
                                if let Err(e) = forward_sender.send(packet) {
                                    error!("Error forwarding packet: {:?}", e);
                                }

                                while let Ok(Some(item)) = upstream.try_next().await {
                                    if let Err(e) = forward_sender.send(item) {
                                        error!("Error forwarding packet: {:?}", e);
                                        break;
                                    }
                                }
                            }
                            Err(e) => {
                                error!("Error receiving packet: {:?}", e);
                            }
                        }
                    } else {
                        error!("Upstream stream ended unexpectedly.");
                        return;
                    }
                }
            }
        }
    });
}

fn shutdown_future(exit: Arc<AtomicBool>) -> impl Future<Output = ()> + Send + 'static {
    async move {
        // Poll until exit flips to true
        while !exit.load(Ordering::Relaxed) {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}

fn make_local_stream<T: Send + Clone + 'static>(
    rx: broadcast::Receiver<T>,
    exit: &Arc<AtomicBool>,
) -> BoxStream<T> {
    let shutdown = shutdown_future(exit.clone());
    let s = BroadcastStream::new(rx)
        .filter_map(|res| futures_util::future::ready(res.ok().map(Ok)))
        .take_until(shutdown);
    Box::pin(s)
}

#[tonic::async_trait]
impl BlockEngineValidator for GrpcServer {
    type SubscribePacketsStream = BoxStream<SubscribePacketsResponse>;

    async fn subscribe_packets(
        &self,
        req: Request<SubscribePacketsRequest>,
    ) -> Result<Response<Self::SubscribePacketsStream>, Status> {
        info!("Received subscribe_packets request from: {:?}", req.remote_addr());
        let peer = req.remote_addr();

        let local_rx = self.packets_sender_from_proxy.subscribe();
        let local_stream = make_local_stream(local_rx, &self.exit);

        let mut upstream = self.get_block_engine_client(peer).await?;
        let up_resp = match upstream.subscribe_packets(req).await {
            Ok(resp) => resp,
            Err(e) => {
                if e.code() != Code::PermissionDenied {
                    return Err(e)
                }

                warn!("Validator is blocked from Bundles at Blockengine: {:?}", peer);
                return Ok(Self::make_response(local_stream));
            }
        };

        forwarder(
            up_resp.into_inner(),
            self.packets_sender_from_blockengine.clone(),
            self.rt.clone(),
            self.exit.clone(),
        );

        Ok(Self::make_response(local_stream))
    }

    type SubscribeBundlesStream = BoxStream<SubscribeBundlesResponse>;

    async fn subscribe_bundles(
        &self,
        req: Request<SubscribeBundlesRequest>,
    ) -> Result<Response<Self::SubscribeBundlesStream>, Status> {
        info!("Received subscribe_bundles request from: {:?}", req.remote_addr());
        let peer = req.remote_addr();

        let local_rx = self.bundles_sender_from_proxy.subscribe();
        let local_stream = make_local_stream(local_rx, &self.exit);

        let mut upstream = self.get_block_engine_client(peer).await?;
        let up_resp = match upstream.subscribe_bundles(req).await {
            Ok(resp) => resp,
            Err(e) => {
                if e.code() != Code::PermissionDenied {
                    return Err(e)
                }

                warn!("Validator is blocked from Packets at Blockengine: {:?}", peer);
                return Ok(Self::make_response(local_stream));
            }
        };

        forwarder(
            up_resp.into_inner(),
            self.bundles_sender_from_blockengine.clone(),
            self.rt.clone(),
            self.exit.clone(),
        );

        Ok(Self::make_response(local_stream))
    }

    async fn get_block_builder_fee_info(
        &self,
        req: Request<BlockBuilderFeeInfoRequest>,
    ) -> Result<Response<BlockBuilderFeeInfoResponse>, Status> {
        info!("Received get_block_builder_fee_info request from: {:?}", req.remote_addr());
        let peer = req.remote_addr();
        let mut upstream = self.get_block_engine_client(peer).await?;

        upstream.get_block_builder_fee_info(req).await
    }
}

#[tonic::async_trait]
impl AuthService for GrpcServer {
    async fn generate_auth_challenge(
        &self,
        req: Request<GenerateAuthChallengeRequest>,
    ) -> Result<Response<GenerateAuthChallengeResponse>, Status> {
        info!("Received generate_auth_challenge request from: {:?}", req.remote_addr());
        let peer = req.remote_addr();
        let mut upstream = self.get_auth_client(peer).await?;
        upstream.generate_auth_challenge(req).await
    }

    async fn generate_auth_tokens(
        &self,
        req: Request<GenerateAuthTokensRequest>,
    ) -> Result<Response<GenerateAuthTokensResponse>, Status> {
        info!("Received generate_auth_tokens request from: {:?}", req.remote_addr());
        let peer = req.remote_addr();
        let mut upstream = self.get_auth_client(peer).await?;
        upstream.generate_auth_tokens(req).await
    }

    async fn refresh_access_token(
        &self,
        req: Request<RefreshAccessTokenRequest>,
    ) -> Result<Response<RefreshAccessTokenResponse>, Status> {
        info!("Received refresh_access_token request from: {:?}", req.remote_addr());
        let peer = req.remote_addr();
        let mut upstream = self.get_auth_client(peer).await?;
        upstream.refresh_access_token(req).await
    }
}

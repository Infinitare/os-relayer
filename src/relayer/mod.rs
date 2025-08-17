mod tpu;
mod staked_nodex_updater_service;
mod forwarder;
mod auth;

use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc};
use std::sync::atomic::AtomicBool;
use std::{fs, thread};
use std::path::PathBuf;
use agave_banking_stage_ingress_types::BankingPacketBatch;
use crossbeam_channel::{Receiver, Sender};
use jwt::{AlgorithmType, PKeyWithDigest};
use log::info;
use openssl::hash::MessageDigest;
use openssl::pkey::{PKey};
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Keypair;
use tonic::transport::Server;
use crate::helper::shutdown_signal;
use crate::protos::auth::auth_service_server::AuthServiceServer;
use crate::protos::relayer::relayer_server::RelayerServer;
use crate::relayer::auth::auth_interceptor::AuthInterceptor;
use crate::relayer::auth::auth_service::AuthServiceImpl;
use crate::relayer::auth::ValidatorAutherImpl;
use crate::relayer::forwarder::Forwarder;
use crate::relayer::tpu::Tpu;
use crate::rpc::load_balancer::LoadBalancer;

pub struct Relayer {
    tpu: Tpu,
    server: tokio::task::JoinHandle<()>,
    forwarder_tasks: Vec<thread::JoinHandle<()>>,
}

impl Relayer {
    pub fn new(
        keypair: &Arc<Keypair>,
        rt: &tokio::runtime::Handle,
        public_ip: &IpAddr,
        grpc_bind_ip: &IpAddr,
        relayer_bind_port: u16,
        signing_key_pem_path: &PathBuf,
        verifying_key_pem_path: &PathBuf,
        tpu_quic_port: u16,
        tpu_quic_fwd_port: u16,
        staked_nodes_overrides: HashMap<Pubkey, u64>,
        rpc_load_balancer: &Arc<LoadBalancer>,
        exit: &Arc<AtomicBool>,
    ) -> (Self, Sender<BankingPacketBatch>, Receiver<BankingPacketBatch>) {
        let private_key = fs::read(&signing_key_pem_path).unwrap_or_else(|_| {
            panic!(
                "Failed to read signing key file: {:?}",
                &verifying_key_pem_path
            )
        });
        let signing_key = PKeyWithDigest {
            digest: MessageDigest::sha256(),
            key: PKey::private_key_from_pem(&private_key).unwrap(),
        };

        let public_key = fs::read(&verifying_key_pem_path).unwrap_or_else(|_| {
            panic!(
                "Failed to read verifying key file: {:?}",
                &verifying_key_pem_path
            )
        });
        let verifying_key = Arc::new(PKeyWithDigest {
            digest: MessageDigest::sha256(),
            key: PKey::public_key_from_pem(&public_key).unwrap(),
        });

        let (tpu, receiver) = Tpu::new(
            keypair,
            tpu_quic_port,
            tpu_quic_fwd_port,
            staked_nodes_overrides,
            rpc_load_balancer,
            exit,
        );

        let (forwarder, forwarder_tasks, sender) = Forwarder::new(
            public_ip,
            tpu_quic_port,
            tpu_quic_fwd_port,
            exit,
        );

        let auth_svc = AuthServiceImpl::new(
            ValidatorAutherImpl::default(),
            signing_key,
            verifying_key.clone(),
            &exit,
        );

        let server_addr = SocketAddr::new(grpc_bind_ip.clone(), relayer_bind_port);
        let exit = exit.clone();
        info!("starting relayer at: {:?}", server_addr);
        let server = rt.spawn(async move {
            Server::builder()
                .add_service(RelayerServer::with_interceptor(
                    forwarder,
                    AuthInterceptor::new(verifying_key.clone(), AlgorithmType::Rs256),
                ))
                .add_service(AuthServiceServer::new(auth_svc))
                .serve_with_shutdown(server_addr, shutdown_signal(exit.clone()))
                .await
                .expect("serve relayer");
        });
        
        (
            Relayer {
                tpu,
                server,
                forwarder_tasks
            },
            sender,
            receiver
        )
    }

    pub async fn join(self) -> thread::Result<()> {
        self.tpu.join()?;
        self.server.await.expect("server join failed");
        for task in self.forwarder_tasks {
            task.join().expect("forwarder task join failed");
        }
        Ok(())
    }
}
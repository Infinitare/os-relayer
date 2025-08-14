use crate::relayer::staked_nodex_updater_service::StakedNodesUpdaterService;
use solana_core::tpu::MAX_QUIC_CONNECTIONS_PER_PEER;
use solana_net_utils::{bind_to_with_config, SocketConfig};
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Keypair;
use solana_streamer::quic::{spawn_server, QuicServerParams};
use solana_streamer::streamer::StakedNodes;
use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr};
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, RwLock};
use std::thread;
use std::thread::JoinHandle;
use agave_banking_stage_ingress_types::BankingPacketBatch;
use crossbeam_channel::Receiver;
use solana_core::banking_trace::BankingTracer;
use solana_core::sigverify::TransactionSigVerifier;
use solana_core::sigverify_stage::SigVerifyStage;
use crate::rpc::load_balancer::LoadBalancer;

pub struct Tpu {
    tasks: Vec<JoinHandle<()>>,
    sigverify_stage: SigVerifyStage,
    staked_nodes_updater_service: StakedNodesUpdaterService,
}

impl Tpu {
    pub const TPU_QUEUE_CAPACITY: usize = 10_000;
    pub const MAX_CONNECTIONS_PER_IPADDR_PER_MIN: u64 = 64;

    pub fn new(
        keypair: &Keypair,
        tpu_quic_port: u16,
        tpu_quic_fwd_port: u16,
        staked_nodes_overrides: HashMap<Pubkey, u64>,
        rpc_load_balancer: &Arc<LoadBalancer>,
        exit: &Arc<AtomicBool>,
    ) -> (Self, Receiver<BankingPacketBatch>) {
        let (tpu_packet_sender, tpu_packet_receiver) = crossbeam_channel::bounded(Self::TPU_QUEUE_CAPACITY);

        let socket_config = SocketConfig::default().reuseport(true);
        let tpu_quic_socket = bind_to_with_config(
            IpAddr::V4(Ipv4Addr::from([0, 0, 0, 0])),
            tpu_quic_port,
            socket_config,
        ).unwrap();
        let tpu_quic_fwd_socket = bind_to_with_config(
            IpAddr::V4(Ipv4Addr::from([0, 0, 0, 0])),
            tpu_quic_fwd_port,
            socket_config,
        ).unwrap();

        let staked_nodes = Arc::new(RwLock::new(StakedNodes::default()));
        let staked_nodes_updater_service = StakedNodesUpdaterService::new(
            exit.clone(),
            rpc_load_balancer.clone(),
            staked_nodes.clone(),
            staked_nodes_overrides,
        );

        let quic_task = spawn_server(
            "quic_streamer_tpu",
            "quic_streamer_tpu",
            tpu_quic_socket,
            keypair,
            tpu_packet_sender.clone(),
            exit.clone(),
            staked_nodes.clone(),
            QuicServerParams{
                max_connections_per_peer: MAX_QUIC_CONNECTIONS_PER_PEER,
                max_connections_per_ipaddr_per_min: Self::MAX_CONNECTIONS_PER_IPADDR_PER_MIN,
                max_staked_connections: 2000,
                max_unstaked_connections: 500,
                ..QuicServerParams::default()
            },
        ).unwrap().thread;

        let quic_fwd_task = spawn_server(
            "quic_streamer_tpu_fwd",
            "quic_streamer_tpu_fwd",
            tpu_quic_fwd_socket,
            keypair,
            tpu_packet_sender.clone(),
            exit.clone(),
            staked_nodes.clone(),
            QuicServerParams{
                max_connections_per_peer: MAX_QUIC_CONNECTIONS_PER_PEER,
                max_connections_per_ipaddr_per_min: Self::MAX_CONNECTIONS_PER_IPADDR_PER_MIN,
                max_staked_connections: 2000,
                max_unstaked_connections: 500,
                ..QuicServerParams::default()
            },
        ).unwrap().thread;

        let (banking_packet_sender, banking_packet_receiver) =
            BankingTracer::new_disabled().create_channel_non_vote();

        let sigverify_stage = SigVerifyStage::new(
            tpu_packet_receiver,
            TransactionSigVerifier::new(banking_packet_sender, None),
            "tpu-verifier",
            "tpu-verifier",
        );

        let tasks = vec![quic_task, quic_fwd_task];

        (
            Tpu {
                tasks,
                sigverify_stage,
                staked_nodes_updater_service,
            },
            banking_packet_receiver
        )
    }

    pub fn join(self) -> thread::Result<()> {
        for task in self.tasks {
            task.join()?;
        }

        self.sigverify_stage.join()?;
        self.staked_nodes_updater_service.join()?;

        Ok(())
    }
}
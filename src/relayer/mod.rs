mod tpu;
mod staked_nodex_updater_service;
mod forwarder;

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::{Arc, RwLock};
use std::sync::atomic::AtomicBool;
use std::thread;
use agave_banking_stage_ingress_types::BankingPacketBatch;
use crossbeam_channel::{Receiver, Sender};
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Keypair;
use crate::relayer::forwarder::Forwarder;
use crate::relayer::tpu::Tpu;
use crate::rpc::load_balancer::LoadBalancer;


pub struct Relayer {
    tpu: Tpu,
    forwarder: Forwarder,
}

impl Relayer {
    pub fn new(
        keypair: &Arc<Keypair>,
        public_ip: &IpAddr,
        tpu_quic_port: u16,
        tpu_quic_fwd_port: u16,
        staked_nodes_overrides: HashMap<Pubkey, u64>,
        rpc_load_balancer: &Arc<LoadBalancer>,
        exit: &Arc<AtomicBool>,
    ) -> (Self, Sender<BankingPacketBatch>, Receiver<BankingPacketBatch>) {
        let (tpu, receiver) = Tpu::new(
            keypair,
            tpu_quic_port,
            tpu_quic_fwd_port,
            staked_nodes_overrides,
            rpc_load_balancer,
            exit,
        );
        
        let (forwarder, sender) = Forwarder::new(
            public_ip,
            tpu_quic_port,
            tpu_quic_fwd_port,
            exit,
        );

        (
            Relayer {
                tpu,
                forwarder,
            },
            sender,
            receiver
        )
    }

    pub fn join(self) -> thread::Result<()> {
        self.tpu.join()?;
        self.forwarder.join()?;
        Ok(())
    }
}
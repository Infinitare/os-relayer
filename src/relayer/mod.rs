mod tpu;
mod staked_nodex_updater_service;

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::sync::atomic::AtomicBool;
use agave_banking_stage_ingress_types::BankingPacketBatch;
use crossbeam_channel::{Receiver, Sender};
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Keypair;
use crate::relayer::tpu::Tpu;
use crate::rpc::load_balancer::LoadBalancer;

pub fn start_relayer_service(
    keypair: &Arc<Keypair>,
    tpu_quic_port: u16,
    tpu_quic_fwd_port: u16,
    staked_nodes_overrides: HashMap<Pubkey, u64>,
    rpc_load_balancer: &Arc<LoadBalancer>,
    exit: &Arc<AtomicBool>,
) -> (Sender<BankingPacketBatch>, Receiver<BankingPacketBatch>) {
    let (tpu, receiver) = Tpu::new(
        keypair,
        tpu_quic_port,
        tpu_quic_fwd_port,
        staked_nodes_overrides,
        rpc_load_balancer,
        exit,
    );
    
    tpu.join().unwrap();
    (_, receiver)
}
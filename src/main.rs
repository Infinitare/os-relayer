mod helper;
mod relayer;
mod blockengine;
mod proxy;
mod rpc;
mod protos;

use std::fs;
use std::net::IpAddr;
use std::path::PathBuf;
use std::sync::{Arc};
use agave_validator::admin_rpc_service::StakedNodesOverrides;
use clap::Parser;
use env_logger::Env;
use log::{info};
use solana_sdk::signature::read_keypair_file;
use solana_sdk::signer::Signer;
use tokio::runtime::Builder;
use crate::blockengine::Blockengine;
use crate::helper::graceful_panic;
use crate::proxy::Proxy;
use crate::relayer::Relayer;
use crate::rpc::load_balancer::LoadBalancer;

#[derive(Parser, Debug)]
#[clap(version)]
struct Args {
    /// Path to keypair file used to authenticate with the backend
    #[arg(long, env)]
    keypair_path: PathBuf,

    /// The private key used to sign tokens by this server.
    #[arg(long, env)]
    signing_key_pem_path: PathBuf,

    /// The public key used to verify tokens by this and other services.
    #[arg(long, env)]
    verifying_key_pem_path: PathBuf,

    /// RPC server the Relayer will connect to for data
    #[arg(
        long,
        env,
        default_value = "http://127.0.0.1:8899"
    )]
    rpc_server: String,

    /// Websocket server the Relayer will connect to for data
    #[arg(
        long,
        env,
        default_value = "ws://127.0.0.1:8900"
    )]
    websocket_server: String,

    /// Bind IP address for GRPC server
    #[arg(long, env, default_value_t = IpAddr::V4(std::net::Ipv4Addr::new(0, 0, 0, 0)))]
    grpc_bind_ip: IpAddr,

    /// Bind port address for GRPC server
    #[arg(long, env, default_value_t = 11_225)]
    blockengine_bind_port: u16,

    /// Bind port address for GRPC server
    #[arg(long, env, default_value_t = 11_226)]
    relayer_bind_port: u16,

    /// Path to staked nodes overrides file
    #[arg(long, env)]
    staked_nodes_overrides: Option<PathBuf>,

    /// Port for TPU QUIC packets
    #[arg(long, env, default_value_t = 11_228)]
    tpu_quic_port: u16,

    /// Port for TPU QUIC Forward packets
    #[arg(long, env, default_value_t = 11_229)]
    tpu_quic_fwd_port: u16,

    /// Public IP address of the validator - if not provided, it will be determined automatically
    #[arg(long, env)]
    public_ip: Option<IpAddr>,

    /// Jito Blockengine the Relayer will connect to - choose the one closest to your Server
    #[arg(long, env)]
    jito_blockengine: String,

    /// Server the Relayer will connect to analyze transactions
    #[arg(long, env)]
    proxy: String,
}

fn main() {
    env_logger::Builder::from_env(Env::new().default_filter_or("info"))
        .format_timestamp_millis()
        .init();

    let args: Args = Args::parse();
    info!("Starting Relayer v{} with Jito Blockengine: {} and Proxy: {}", env!("CARGO_PKG_VERSION"), args.jito_blockengine, args.proxy);
    info!("Starting TPU Quic Port: {} and TPU Quic Forward Port: {}", args.tpu_quic_port, args.tpu_quic_fwd_port);

    let keypair =
        Arc::new(read_keypair_file(args.keypair_path).expect("keypair file does not exist"));
    info!("Using Keypair: {}", keypair.pubkey());

    let staked_nodes_overrides = match args.staked_nodes_overrides {
        None => StakedNodesOverrides::default(),
        Some(p) => {
            let file = fs::File::open(&p).expect(&format!(
                "Failed to open staked nodes overrides file: {:?}",
                &p
            ));
            serde_yaml::from_reader(file).expect(&format!(
                "Failed to read staked nodes overrides file: {:?}",
                &p,
            ))
        }
    };
    let public_ip = if args.public_ip.is_some() {
        args.public_ip.unwrap()
    } else {
        let entrypoint = solana_net_utils::parse_host_port("entrypoint.mainnet-beta.solana.com:8001")
            .expect("parse entrypoint");
        info!(
            "Contacting {} to determine the validator's public IP address",
            entrypoint
        );
        solana_net_utils::get_public_ip_addr(&entrypoint).expect("get public ip address")
    };

    let exit = graceful_panic(None);
    let rt = Builder::new_multi_thread()
        .enable_all()
        .disable_lifo_slot()
        .worker_threads(256)
        .thread_stack_size(5 * 1024 * 1024)
        .build()
        .unwrap();

    let (rpc_load_balancer, load_balancer_threads, _) = LoadBalancer::new(&vec![(args.rpc_server, args.websocket_server)], &exit);
    let rpc_load_balancer_arc = Arc::new(rpc_load_balancer);

    let (relayer, packet_sender, packet_receiver) = Relayer::new(
        &keypair,
        &rt.handle(),
        &public_ip,
        &args.grpc_bind_ip,
        args.blockengine_bind_port,
        &args.signing_key_pem_path,
        &args.verifying_key_pem_path,
        args.tpu_quic_port,
        args.tpu_quic_fwd_port,
        staked_nodes_overrides.staked_map_id,
        &rpc_load_balancer_arc.clone(),
        &exit,
    );

    let (blockengine, jito_bundle_sender, jito_bundle_receiver, jito_packets_sender, jito_packets_receiver) = Blockengine::new(
        &rt.handle(),
        &args.grpc_bind_ip,
        args.relayer_bind_port,
        args.jito_blockengine,
        &exit,
    );


    let proxy = Proxy::new(
        &keypair,
        &rt.handle(),
        &args.proxy,
        packet_sender,
        packet_receiver,
        jito_bundle_sender,
        jito_bundle_receiver,
        jito_packets_sender,
        jito_packets_receiver,
        &exit,
    );
    
    rt.block_on(async move {
        relayer.join().await.expect("failed to join relayer");
        blockengine.join().await.expect("failed to join blockengine");
        proxy.join().await;
    });
    load_balancer_threads.join().expect("failed to join load_balancer threads");

    exit.store(true, std::sync::atomic::Ordering::Relaxed);
    info!("Relayer service has been stopped gracefully.");
}

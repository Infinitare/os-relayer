use std::cmp::min;
use std::net::{IpAddr, Ipv4Addr};
use solana_perf::packet::{Meta, Packet, PacketFlags, PacketRef, PACKET_DATA_SIZE};
use crate::protos::packet::{Meta as ProtoMeta, Packet as ProtoPacket, PacketFlags as ProtoPacketFlags};

pub fn packet_to_proto_packet(p: PacketRef) -> Option<ProtoPacket> {
    Some(ProtoPacket {
        data: p.data(..)?.to_vec(),
        meta: Some(ProtoMeta {
            size: p.meta().size as u64,
            addr: p.meta().addr.to_string(),
            port: p.meta().port as u32,
            flags: Some(ProtoPacketFlags {
                discard: p.meta().discard(),
                forwarded: p.meta().forwarded(),
                repair: p.meta().repair(),
                simple_vote_tx: p.meta().is_simple_vote_tx(),
                tracer_packet: p.meta().is_perf_track_packet(),
                from_staked_node: p.meta().is_from_staked_node(),
            }),
            sender_stake: 0,
        }),
    })
}

const UNKNOWN_IP: IpAddr = IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0));

pub fn proto_packet_to_packet(p: &ProtoPacket) -> Packet {
    let mut data = [0; PACKET_DATA_SIZE];
    let copy_len = min(data.len(), p.data.len());
    data[..copy_len].copy_from_slice(&p.data[..copy_len]);
    let mut packet = Packet::new(data, Meta::default());
    let p = p.clone();
    if let Some(meta) = p.meta {
        packet.meta_mut().size = meta.size as usize;
        packet.meta_mut().addr = meta.addr.parse().unwrap_or(UNKNOWN_IP);
        packet.meta_mut().port = meta.port as u16;
        if let Some(flags) = meta.flags {
            if flags.simple_vote_tx {
                packet.meta_mut().flags.insert(PacketFlags::SIMPLE_VOTE_TX);
            }
            if flags.forwarded {
                packet.meta_mut().flags.insert(PacketFlags::FORWARDED);
            }
            if flags.repair {
                packet.meta_mut().flags.insert(PacketFlags::REPAIR);
            }
            if flags.from_staked_node {
                packet
                    .meta_mut()
                    .flags
                    .insert(PacketFlags::FROM_STAKED_NODE);
            }
        }
    }
    packet
}

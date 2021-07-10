mod benchmarks;

use anyhow::{bail, Context, Result};
use benchmarks::Benchmarks;
use log::{error, info};
use md5::{Digest, Md5};
use rand::{thread_rng, RngCore};
use std::{
    env::args,
    mem::size_of,
    net::{IpAddr, Ipv4Addr, SocketAddr, SocketAddrV4, UdpSocket},
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    thread,
};

const PACKET_SIZE: usize = 1200;
const CHECKSUM_SIZE: usize = 16;
const KEY_SIZE: usize = size_of::<u64>();

fn main() {
    env_logger::init();

    let port: u16 = {
        let port_s = args().nth(1).expect("PORT MUST BE SPECIFIED");
        port_s.parse().expect("PORT MUST BE NUMBER")
    };
    let concurrency: usize = {
        let concurrency_s = args().nth(2).expect("CONCURRENCY MUST BE SPECIFIED");
        concurrency_s.parse().expect("CONCURRENCY MUST BE NUMBER")
    };
    let socket_addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), port));

    let benchmarks = Benchmarks::new();

    let client_interrupted = Arc::new(AtomicBool::new(false));
    signal_hook::flag::register(signal_hook::consts::SIGINT, Arc::clone(&client_interrupted))
        .expect("FAILED TO SETUP SIGNAL HANDLER");

    let clients_stats = Arc::new(Mutex::new(Stats::default()));
    let clients_alive = Arc::new(AtomicUsize::new(concurrency));
    let server_thread = {
        let clients_alive = clients_alive.to_owned();
        let benchmarks = benchmarks.to_owned();
        thread::spawn(move || echo_server(socket_addr, clients_alive, benchmarks))
    };
    let mut client_threads = Vec::with_capacity(concurrency);
    for _ in 0..concurrency {
        let clients_stats = clients_stats.to_owned();
        let client_interrupted = client_interrupted.to_owned();
        let benchmarks = benchmarks.to_owned();
        client_threads.push(thread::spawn(move || {
            echo_client(socket_addr, clients_stats, client_interrupted, benchmarks)
        }));
    }
    for thread in client_threads {
        if let Err(err) = thread.join() {
            error!("Client thread join err: {:?}", err);
        }
        clients_alive.fetch_sub(1, Ordering::SeqCst);
    }
    if let Err(err) = server_thread.join() {
        error!("Client thread join err: {:?}", err);
    }
}

fn echo_server(socket_addr: SocketAddr, clients_alive: Arc<AtomicUsize>, benchmarks: Benchmarks) {
    let mut socket = UdpSocket::bind(socket_addr).expect("Bind udp socket error");
    let mut total = 0;
    let mut succeed = 0;

    while clients_alive.load(Ordering::SeqCst) > 0 {
        total += 1;
        if let Err(err) = _echo_server(&mut socket, &benchmarks) {
            error!("Echo error: {} ({} / {})", err, succeed, total);
        } else {
            succeed += 1;
            info!("Echo ok: ({} / {})", succeed, total);
        }
    }

    fn _echo_server(socket: &mut UdpSocket, benchmarks: &Benchmarks) -> Result<()> {
        let mut buf = [0u8; PACKET_SIZE];
        let (received, src) = socket
            .recv_from(&mut buf)
            .with_context(|| "Receive error")?;
        if received < CHECKSUM_SIZE + KEY_SIZE {
            bail!("Short received: {}", received);
        }

        {
            let mut key_buf = [0u8; KEY_SIZE];
            let key_part =
                &mut buf[(received - CHECKSUM_SIZE - KEY_SIZE)..(received - CHECKSUM_SIZE)];
            key_buf.copy_from_slice(key_part);
            if let Some(duration) = benchmarks.done(u64::from_le_bytes(key_buf)) {
                info!("*** Server benchmarks: {} ms", duration.as_millis());
            }
        }

        {
            let checksum = &buf[(received - CHECKSUM_SIZE)..received];
            let expected = {
                let mut hasher = Md5::new();
                hasher.update(&buf[..(received - CHECKSUM_SIZE)]);
                hasher.finalize()
            };
            if checksum != expected.as_slice() {
                bail!("Invalid checksum");
            }
        }

        {
            let key_part =
                &mut buf[(received - CHECKSUM_SIZE - KEY_SIZE)..(received - CHECKSUM_SIZE)];
            key_part.copy_from_slice(&benchmarks.new_key().to_le_bytes());
        }

        socket
            .send_to(&buf[..received], src)
            .with_context(|| "Send error")?;
        Ok(())
    }
}

#[derive(Default, Clone, Debug)]
struct Stats {
    total: u64,
    succeed: u64,
}

fn echo_client(
    server_addr: SocketAddr,
    stats: Arc<Mutex<Stats>>,
    interrupted: Arc<AtomicBool>,
    benchmarks: Benchmarks,
) {
    if let Err(err) = thread_worker(server_addr, stats, interrupted, benchmarks) {
        error!("Start thread worker error: {}", err);
    }

    fn thread_worker(
        server_addr: SocketAddr,
        stats: Arc<Mutex<Stats>>,
        interrupted: Arc<AtomicBool>,
        benchmarks: Benchmarks,
    ) -> Result<()> {
        let mut udp_socket = UdpSocket::bind((IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), 0))?;
        udp_socket.connect(server_addr)?;

        while !interrupted.load(Ordering::SeqCst) {
            stats.lock().unwrap().total += 1;
            if let Err(err) = _echo_client(&mut udp_socket, &benchmarks) {
                let stats = stats.lock().unwrap();
                error!("Echo error: {} ({} / {})", err, stats.succeed, stats.total);
            } else {
                let mut stats = stats.lock().unwrap();
                stats.succeed += 1;
                info!("Echo ok: ({} / {})", stats.succeed, stats.total);
            }
        }

        Ok(())
    }

    fn _echo_client(udp_socket: &mut UdpSocket, benchmarks: &Benchmarks) -> anyhow::Result<()> {
        let mut rng = thread_rng();

        let mut send_buf = [0u8; PACKET_SIZE];
        let mut recv_buf = [0u8; PACKET_SIZE];

        {
            let mut content = &mut send_buf[..(PACKET_SIZE - CHECKSUM_SIZE - KEY_SIZE)];
            rng.fill_bytes(&mut content);
        }

        {
            let key_buf = &mut send_buf
                [(PACKET_SIZE - CHECKSUM_SIZE - KEY_SIZE)..(PACKET_SIZE - CHECKSUM_SIZE)];
            key_buf.copy_from_slice(&benchmarks.new_key().to_le_bytes());
        }

        {
            let mut hasher = Md5::new();
            hasher.update(&send_buf[..(PACKET_SIZE - CHECKSUM_SIZE)]);
            let checksum = &mut send_buf[(PACKET_SIZE - CHECKSUM_SIZE)..PACKET_SIZE];
            checksum.copy_from_slice(hasher.finalize().as_slice());
        }

        udp_socket.send(&send_buf)?;
        let received = udp_socket.recv(&mut recv_buf)?;

        if received != send_buf.len() {
            bail!("Short received: {}", received);
        }

        {
            let mut key_buf = [0u8; KEY_SIZE];
            key_buf.copy_from_slice(
                &recv_buf[(received - CHECKSUM_SIZE - KEY_SIZE)..(received - CHECKSUM_SIZE)],
            );
            if let Some(duration) = benchmarks.done(u64::from_le_bytes(key_buf)) {
                info!("*** Client benchmarks: {} ms", duration.as_millis());
            }
        }

        if recv_buf[..(received - CHECKSUM_SIZE - KEY_SIZE)]
            != send_buf[..(received - CHECKSUM_SIZE - KEY_SIZE)]
        {
            bail!("Invalid packet data");
        }

        if recv_buf[(received - CHECKSUM_SIZE)..received]
            != send_buf[(received - CHECKSUM_SIZE)..received]
        {
            bail!("Invalid packet checksum");
        }

        Ok(())
    }
}

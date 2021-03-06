use anyhow::{bail, Context, Result};
use core::slice;
use log::{error, info};
use md5::{Digest, Md5};
use nix::sys::{
    socket::{recvmmsg, InetAddr, MsgFlags, RecvMmsgData, SockAddr},
    uio::IoVec,
};
use rand::{thread_rng, RngCore};
use std::{
    convert::TryInto,
    env::args,
    mem::{size_of, zeroed},
    net::{IpAddr, Ipv4Addr, SocketAddr, SocketAddrV4, UdpSocket},
    os::unix::prelude::AsRawFd,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

const PACKET_SIZE: usize = 1200;
const CHECKSUM_SIZE: usize = 16;
const KEY_SIZE: usize = size_of::<u128>();

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

    let clients_stats = Arc::new(Mutex::new(Stats::default()));
    let clients_alive = Arc::new(AtomicUsize::new(concurrency));
    let mut core_ids = core_affinity::get_core_ids().expect("Get core ids error");
    info!("Core ids: {}", core_ids.len());
    let server_thread = {
        let clients_alive = clients_alive.to_owned();
        let core_id = core_ids.pop().expect("Empty cores");
        thread::spawn(move || {
            core_affinity::set_for_current(core_id);
            echo_server(socket_addr, clients_alive)
        })
    };
    let mut client_threads = Vec::with_capacity(concurrency);
    for i in 0..concurrency {
        let clients_stats = clients_stats.to_owned();
        let core_id = core_ids
            .iter()
            .nth(i % core_ids.len())
            .expect("Need more core")
            .to_owned();
        client_threads.push(thread::spawn(move || {
            core_affinity::set_for_current(core_id);
            echo_client(socket_addr, clients_stats)
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

fn echo_server(socket_addr: SocketAddr, clients_alive: Arc<AtomicUsize>) {
    let mut socket = UdpSocket::bind(socket_addr).expect("Bind udp socket error");
    let mut total = 0;
    let mut succeed = 0;

    while clients_alive.load(Ordering::SeqCst) > 0 {
        total += 1;
        if let Err(err) = _echo_server(&mut socket) {
            error!("Server echo error: {} ({} / {})", err, succeed, total);
        } else {
            succeed += 1;
            info!("Server echo ok: ({} / {})", succeed, total);
        }
    }

    fn _echo_server(socket: &mut UdpSocket) -> Result<()> {
        // let (received, src) = socket
        //     .recv_from(&mut buf)
        //     .with_context(|| "Receive error")?;
        const MSGLEN: usize = 16;
        let mut bufs = [[0u8; PACKET_SIZE]; MSGLEN];
        let mut iovecs: [[IoVec<&mut [u8]>; 1]; MSGLEN] = unsafe { zeroed() };
        bufs.iter_mut()
            .map(|buf| unsafe { slice::from_raw_parts_mut(buf.as_mut_ptr(), buf.len()) })
            .enumerate()
            .for_each(|(idx, buf)| iovecs[idx] = [IoVec::from_mut_slice(buf)]);
        let mut msgs: [RecvMmsgData<&[IoVec<&mut [u8]>]>; MSGLEN] = unsafe { zeroed() };
        iovecs.iter().enumerate().for_each(|(idx, iovec)| {
            msgs[idx] = RecvMmsgData {
                iov: iovec,
                cmsg_buffer: None,
            }
        });
        let received_messages = recvmmsg(
            socket.as_raw_fd(),
            msgs.iter_mut(),
            MsgFlags::empty(),
            Some(Duration::from_millis(0).into()),
        )?;

        info!("*** received_messages: {}", received_messages.len());
        for (idx, received_message) in received_messages.into_iter().enumerate() {
            let src = match received_message.address {
                Some(SockAddr::Inet(inet_addr)) => inet_addr.to_std(),
                _ => panic!("Invalid address: {:?}", received_message.address),
            };
            let received = received_message.bytes;
            let buf: &mut [u8] =
                unsafe { slice::from_raw_parts_mut(bufs[idx].as_mut_ptr(), bufs[idx].len()) };

            if received < CHECKSUM_SIZE + KEY_SIZE {
                bail!("Short received: {}", received);
            }

            {
                let mut key_buf = [0u8; KEY_SIZE];
                let key_part =
                    &mut buf[(received - CHECKSUM_SIZE - KEY_SIZE)..(received - CHECKSUM_SIZE)];
                key_buf.copy_from_slice(key_part);
                let duration = SystemTime::now()
                    .duration_since(
                        UNIX_EPOCH
                            + Duration::from_millis(
                                u128::from_le_bytes(key_buf)
                                    .try_into()
                                    .expect("Cannot convert millis to u64"),
                            ),
                    )
                    .expect("Wrong time system");
                info!("*** Server benchmarks: {} ms", duration.as_millis());
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
                key_part.copy_from_slice(
                    &SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .expect("Never before 1970")
                        .as_millis()
                        .to_le_bytes(),
                );
            }

            socket
                .send_to(&buf[..received], src)
                .with_context(|| "Send error")?;
        }
        Ok(())
    }
}

#[derive(Default, Clone, Debug)]
struct Stats {
    total: u64,
    succeed: u64,
}

fn echo_client(server_addr: SocketAddr, stats: Arc<Mutex<Stats>>) {
    if let Err(err) = thread_worker(server_addr, stats) {
        error!("Start thread worker error: {}", err);
    }

    fn thread_worker(server_addr: SocketAddr, stats: Arc<Mutex<Stats>>) -> Result<()> {
        let mut udp_socket = UdpSocket::bind((IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), 0))?;
        udp_socket.connect(server_addr)?;

        loop {
            stats.lock().unwrap().total += 1;
            if let Err(err) = _echo_client(&mut udp_socket) {
                let stats = stats.lock().unwrap();
                error!(
                    "Client echo error: {} ({} / {})",
                    err, stats.succeed, stats.total
                );
            } else {
                let mut stats = stats.lock().unwrap();
                stats.succeed += 1;
                info!("Client echo ok: ({} / {})", stats.succeed, stats.total);
            }
        }
    }

    fn _echo_client(udp_socket: &mut UdpSocket) -> anyhow::Result<()> {
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
            key_buf.copy_from_slice(
                &SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .expect("Never before 1970")
                    .as_millis()
                    .to_le_bytes(),
            );
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
            let duration = SystemTime::now()
                .duration_since(
                    UNIX_EPOCH
                        + Duration::from_millis(
                            u128::from_le_bytes(key_buf)
                                .try_into()
                                .expect("Cannot convert millis to u64"),
                        ),
                )
                .expect("Wrong time system");
            info!("*** Client benchmarks: {} ms", duration.as_millis());
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

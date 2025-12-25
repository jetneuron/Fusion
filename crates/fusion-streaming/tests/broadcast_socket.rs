use std::net::{Ipv4Addr, SocketAddr, UdpSocket};
use std::os::fd::AsRawFd;

use libc;
use socket2::{Domain, Protocol, Socket, Type};

#[test]
fn test() -> std::io::Result<()> {
    let socket = UdpSocket::bind("0.0.0.0:0")?;
    socket.set_multicast_ttl_v4(1)?;

    let multicast_addr = "239.0.0.1:12345";
    socket.send_to("Hello, multicast!".as_bytes(), multicast_addr)?;
    Ok(())
}

#[test]
fn test_listener() -> std::io::Result<()> {
    // 创建一个 socket 并设置 SO_REUSEADDR 和 SO_REUSEPORT 选项
    let socket = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;
    socket.set_reuse_address(true)?;

    let fd = socket.as_raw_fd();
    unsafe {
        let reuse: libc::c_int = 1;
        if libc::setsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_REUSEPORT,
            &reuse as *const _ as *const libc::c_void,
            std::mem::size_of_val(&reuse) as libc::socklen_t,
        ) != 0
        {
            return Err(std::io::Error::last_os_error());
        }
    }

    // 绑定到地址和端口
    let addr = SocketAddr::from(([0, 0, 0, 0], 12345));
    socket.bind(&addr.into())?;

    // 将 socket 转换为 std::net::UdpSocket
    let udp_socket = std::net::UdpSocket::from(socket);

    // 加入多播组
    let multicast_addr = Ipv4Addr::new(239, 0, 0, 1);
    udp_socket.join_multicast_v4(&multicast_addr, &Ipv4Addr::UNSPECIFIED)?;

    let mut buf = [0; 1024];
    loop {
        let (len, addr) = udp_socket.recv_from(&mut buf)?;
        println!(
            "Received from {}: {}",
            addr,
            String::from_utf8_lossy(&buf[..len])
        );
    }
}

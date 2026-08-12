use fusion_unit_sdk::graph::types::ComputingUnit;
use fusion_unit_sdk::runtime::logical::LogicalTask;
use fusion_unit_sdk::units::dev::create_dev_context;
use fusion_unit_ssh::init_plugin;
use serde_json::json;
use ssh2::Session;
use std::io::{BufRead, BufReader};
use std::net::TcpStream;
use std::path::Path;
use std::thread;

#[test]
pub fn basic_ssh_command() -> Result<(), Box<dyn std::error::Error>> {
    // Connect to the local SSH server
    let server_port = "<ip:port>";

    let tcp = TcpStream::connect(server_port).unwrap();
    let mut sess = Session::new().unwrap();
    sess.set_tcp_stream(tcp);
    sess.handshake().unwrap();

    let username = "<your_username>";
    let pub_key = Some(Path::new("<your_pub_key_location>"));
    let priv_key = Path::new("<your_priv_key_location>");

    sess.userauth_pubkey_file(username, pub_key, priv_key, None)
        .unwrap();
    assert!(sess.authenticated());

    if !sess.authenticated() {
        eprintln!("Authentication failed");
        return Ok(());
    }

    println!("Authentication succeeded!");

    let mut channel = sess.channel_session()?;
    channel.exec("tail -f /tmp/data.log")?;

    let reader = BufReader::new(channel.clone());
    for line in reader.lines() {
        match line {
            Ok(line) => println!("{}", line),
            Err(e) => {
                eprintln!("Error reading line: {}", e);
                break;
            }
        }
    }

    channel.send_eof()?;
    channel.wait_eof()?;
    channel.close()?;
    channel.wait_close()?;
    Ok(())
}

#[tokio::test]
async fn ssh_basic_test() {
    let conf = json!({
        "host": "54.254.148.21",
        "port": 59522,
        "user": "ec2-user",
        "private_key": "/Users/nigel/.ssh/company-ssh-key/tunnel_ssh_rsa/id_rsa",
        "shell": "tail -f /tmp/data.log",
        "separator": "\t"
    });
    let unit = ComputingUnit::new("test_id", "SSHUnitTask").with_config(conf);

    let logical_task = create_test_unit(unit.clone());
    let context = create_dev_context(unit);
    let context_ptr = Box::into_raw(Box::new(context.0));
    async move {
        let mut m = context.1;
        let mut idx = 0;
        while let Some(row) = m.recv().await {
            if idx == 0 {
                row.display_column_names();
            }
            idx += 1;
            println!("{}", row);
        }
    }
    .await;
    logical_task
        .internal_launch(context_ptr)
        .expect("fail")
        .await
        .expect("fail");
}

fn create_test_unit(unit: ComputingUnit) -> Box<dyn LogicalTask> {
    let cloned_unit = unit.clone();
    let type_name = cloned_unit.get_type();
    let plugin = init_plugin();
    plugin.register_units();
    plugin
        .create(unit)
        .expect(format!("Failed to create Graph unit plugin: {}", type_name).as_str())
}

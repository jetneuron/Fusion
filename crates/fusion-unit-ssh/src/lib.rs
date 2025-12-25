use anyhow::bail;
use fusion_derive::LogicalTask;
use fusion_unit_sdk::graph::types::{ComputingUnit, Context, InitUnit, MapUnit, SourceUnit};
use fusion_unit_sdk::proto::transfer::{Column, DataType, Row};
use fusion_unit_sdk::row::types::ColumnDescriptor;
use fusion_unit_sdk::row::utils::RAW_STR;
use fusion_unit_sdk::runtime::{UnitError, UnitResult};
use fusion_unit_sdk::{GraphUnitPlugin, UnitManifest};
use protobuf::EnumOrUnknown;
use ssh2::Session;
use std::future::Future;
use std::io::{BufRead, BufReader};
use std::net::{SocketAddr, TcpStream};
use std::path::Path;
use std::sync::Arc;
use tokio::sync::mpsc;

#[unsafe(no_mangle)]
pub extern "C" fn init_plugin() -> Box<dyn GraphUnitPlugin> {
    Box::new(SSHUnitPlugin {})
}

pub struct SSHUnitPlugin {}

impl GraphUnitPlugin for SSHUnitPlugin {
    fn register_units(&self) -> UnitManifest {
        let mut unit_manifest = UnitManifest::default();
        SSHUnitTask::register_unit(&mut unit_manifest, &self.plugin_version());
        // ... Register other units ...
        unit_manifest
    }

    fn plugin_version(&self) -> &str {
        "1.0.0"
    }
}

#[derive(Default, LogicalTask)]
pub struct SSHUnitTask {
    /// ssh host，native shell execution if localhost
    host: String,
    /// ssh port
    port: u32,
    /// login user
    user: Option<String>,
    /// login password
    password: Option<String>,
    /// login private key location
    private_key: Option<String>,
    /// login public key location
    public_key: Option<String>,
    /// key passphrase
    passphrase: Option<String>,
    /// shell script to execute
    shell: Option<String>,
    /// parse as column if not empty
    field_names: Option<Vec<ColumnDescriptor>>,
    /// parse as column with specify separator
    separator: Option<String>,
    /// buffer size
    buffer_size: Option<usize>,
    /// row count to sample.
    max_rows: Option<usize>,

    session: Option<Session>,
}

impl InitUnit for SSHUnitTask {
    fn init(&mut self, unit: ComputingUnit) {
        unit.get_config().map(|c| {
            self.host = c["host"].as_str().unwrap_or("127.0.0.1").to_string();
            self.port = c["port"].as_u64().unwrap_or(22) as u32;
            self.user = c["user"].as_str().map(|u| u.to_string());
            self.password = c["password"].as_str().map(|p| p.to_string());
            self.private_key = c["private_key"].as_str().map(|p| p.to_string());
            self.public_key = c["public_key"].as_str().map(|p| p.to_string());
            self.passphrase = c["passphrase"].as_str().map(|p| p.to_string());
            self.shell = c["shell"].as_str().map(|p| p.to_string());
            self.separator = c["separator"].as_str().map(|p| p.to_string());
            self.buffer_size = c["buffer_size"].as_u64().map(|p| p as usize);
            self.max_rows = c["max_rows"].as_u64().map(|p| p as usize);
        });

        if !unit.is_source() {
            let session = self.ssh_connect();
            self.session = Some(session);
        }
    }
}

impl SourceUnit for SSHUnitTask {
    fn launch(
        &self,
        ctx: Arc<Context>,
    ) -> anyhow::Result<impl Future<Output = UnitResult<()>> + Send> {
        let session = self.ssh_connect();
        let mut row_fn = self.generate_row_fn();
        let buffer_size = self.buffer_size.unwrap_or(256);
        let (tx, mut rx) = mpsc::channel(buffer_size);
        let cloned_shell = self.shell.clone();
        let max_rows = self.max_rows.unwrap_or_else(|| 0);
        if !session.authenticated() {
            panic!("Fail to authenticate");
        }

        tokio::task::spawn_blocking(move || {
            match cloned_shell {
                None => {}
                Some(shell) => {
                    let mut channel = session
                        .channel_session()
                        .expect("Fail to create session channel");
                    channel.exec(&shell).expect("Fail to exec shell");

                    let reader = BufReader::new(channel.clone());
                    if max_rows > 0 {
                        let mut row_num = 0;
                        for line in reader.lines() {
                            match line {
                                Ok(line) => tx.blocking_send(line).expect("Fail to send line"),
                                Err(e) => {
                                    eprintln!("Error reading line: {}", e);
                                    break;
                                }
                            }
                            row_num += 1;
                            if row_num >= max_rows {
                                break;
                            }
                        }
                    } else {
                        for line in reader.lines() {
                            match line {
                                Ok(line) => tx.blocking_send(line).expect("Fail to send line"),
                                Err(e) => {
                                    eprintln!("Error reading line: {}", e);
                                    break;
                                }
                            }
                        }
                    }

                    channel
                        .send_eof()
                        .expect("Fail to send eof to shell channel");
                    channel.close().expect("Fail to close shell channel");
                    if channel.eof() {
                        channel
                            .wait_close()
                            .expect("Fail to wait shell channel closed");
                    }
                }
            };
        });
        Ok(async move {
            while let Some(line) = rx.recv().await {
                let mut row = Row::new();
                row_fn(&mut row, line);
                ctx.send(row).await;
            }
            Ok(())
        })
    }
}

impl SSHUnitTask {
    fn generate_row_fn(&self) -> Box<dyn FnMut(&mut Row, String) + Send> {
        let row_fn: Box<dyn FnMut(&mut Row, String) + Send> = match self.separator.clone() {
            None => Box::new(move |row: &mut Row, line: String| {
                row.mask = RAW_STR;
                row.raw = line.as_bytes().to_vec();
            }),
            Some(separator) => match self.field_names.clone() {
                None => Box::new(move |row: &mut Row, line: String| {
                    let columns = line
                        .split(separator.as_str())
                        .enumerate()
                        .map(|(idx, s)| {
                            let mut c = Column::new();
                            c.field = format!("c{}", idx);
                            c.dt = EnumOrUnknown::from(DataType::str);
                            c.str_val = s.to_string();
                            c
                        })
                        .collect();
                    row.columns = columns;
                }),
                Some(fields) => Box::new(move |row: &mut Row, line: String| {
                    let columns = line
                        .split(&separator)
                        .collect::<Vec<&str>>()
                        .iter()
                        .enumerate()
                        .map(|(idx, s)| {
                            let mut column = Column::new();
                            column.field = fields[idx].name.clone();
                            column.dt = EnumOrUnknown::from(fields[idx].data_type);
                            column
                        })
                        .collect::<Vec<Column>>();
                    row.columns = columns;
                }),
            },
        };
        row_fn
    }

    fn ssh_connect(&self) -> Session {
        let addr = format!("{}:{}", self.host, self.port);
        let addr = addr.parse::<SocketAddr>().expect("Invalid address");
        let tcp = TcpStream::connect_timeout(&addr, std::time::Duration::from_secs(20))
            .expect("Failed to connect to server via TCP");
        let mut session = Session::new().expect("Fail to create session");
        session.set_tcp_stream(tcp);
        session.handshake().expect("Fail to handshake");

        let user_name = self.user.clone().unwrap_or(String::from("root"));
        match self.password.as_ref() {
            None => {
                let private_key = self.private_key.clone().expect("Fail to get private key");
                let passphrase: Option<&str> = self.passphrase.as_ref().map(String::as_str);
                if let Some(public_key) = self.public_key.as_ref() {
                    session
                        .userauth_pubkey_file(
                            &user_name,
                            Some(Path::new(public_key)),
                            Path::new(&private_key),
                            passphrase,
                        )
                        .expect("Fail to authenticate user");
                } else {
                    session
                        .userauth_pubkey_file(&user_name, None, Path::new(&private_key), passphrase)
                        .expect("Fail to authenticate user");
                }
            }
            Some(pwd) => {
                session
                    .userauth_password(&user_name, pwd)
                    .expect("Fail to authenticate");
            }
        }
        session
    }
}

impl MapUnit for SSHUnitTask {
    fn compute<'life0, 'async_trait>(
        &'life0 self,
        row: Row,
        ctx: &'life0 Context,
    ) -> anyhow::Result<impl Future<Output = UnitResult<()>> + Send>
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        let mut session = self.session.clone().unwrap();
        match session.channel_session() {
            Ok(mut channel) => {
                let shell = self.shell.clone().unwrap();
                Ok(async move {
                    channel.exec(&shell).map_err(SshErrorWrapper)?;
                    Ok(())
                })
            }
            Err(err) => {
                bail!("Fail to create session: {}", err);
            }
        }
    }
}

#[derive(Debug)]
struct SshErrorWrapper(pub ssh2::Error);

impl From<SshErrorWrapper> for UnitError {
    fn from(value: SshErrorWrapper) -> Self {
        UnitError::Unknown(value.0.to_string())
    }
}

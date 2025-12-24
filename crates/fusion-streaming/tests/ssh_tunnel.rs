use std::process::Command;
use std::thread;

#[test]
fn test() {
    use std::process::Command;
    let program = "/usr/bin/ssh";

    let mut child = Command::new(program)
        .arg("-v")
        .arg("-N")
        .arg("-S")
        .arg("none")
        .arg("-o")
        .arg("ControlMaster=no")
        .arg("-o")
        .arg("ExitOnForwardFailure=yes")
        .arg("-o")
        .arg("ConnectTimeout=10")
        .arg("-o")
        .arg("StrictHostKeyChecking=no")
        .arg("-o")
        .arg("NumberOfPasswordPrompts=3")
        .arg("-F")
        .arg("/Applications/Medis.app/Contents/Resources/ssh_config")
        .arg("-i")
        .arg("/Users/nigel/.ssh/company-ssh-key/tunnel_ssh_rsa/id_rsa")
        .arg("-p")
        .arg("59522")
        .arg("root@10.22.5.221")
        .arg("-L")
        .arg("33412:r-j6c2odggsyfkswjs7r.redis.rds.aliyuncs.com:6379")
        .spawn().unwrap_or_else(|e| {
        panic!("failed to execute process: {}", e)
    });

    let join_handle = thread::spawn(move || {
        let pid = child.id();
        println!("================= PID: {}", pid);
        child.wait().unwrap();
    });

    join_handle.join().unwrap();
}
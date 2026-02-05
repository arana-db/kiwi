use std::path::PathBuf;
use std::env;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
    tonic_prost_build::configure()
        .file_descriptor_set_path(out_dir.join("raft_proto_descriptor.bin"))
        .build_server(true)
        .build_client(true)
        .compile_protos(
            &[
                "proto/types.proto",
                "proto/raft.proto",
                "proto/admin.proto",
                "proto/client.proto",
            ],
            &["proto"],
        )?;
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_prost_build::configure()
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

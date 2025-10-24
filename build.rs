fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protoc_path = protoc_bin_vendored::protoc_bin_path()?;
    unsafe {
        // Edition 2024 requires opting into environment mutation explicitly.
        std::env::set_var("PROTOC", protoc_path);
    }

    tonic_build::configure()
        .build_client(true)
        .build_server(true)
        .compile(&["proto/rivetdb.proto"], &["proto"])?;
    Ok(())
}

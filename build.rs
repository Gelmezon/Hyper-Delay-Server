fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_server(true)
        .build_client(false)
        .compile(&["proto/delay.proto"], &["proto"])?;
    println!("cargo:rerun-if-changed=proto/delay.proto");
    Ok(())
}

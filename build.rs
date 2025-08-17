use tonic_build::configure;

fn main() {
    const PROTOC_ENVAR: &str = "PROTOC";
    if std::env::var(PROTOC_ENVAR).is_err() {
        unsafe {
            #[cfg(not(windows))]
            std::env::set_var(PROTOC_ENVAR, protobuf_src::protoc());
        }
    }

    configure()
        .compile(
            &[
                "src/protos/protos/inf.proto",
                "src/protos/protos/auth.proto",
                "src/protos/protos/block_engine.proto",
                "src/protos/protos/packet.proto",
                "src/protos/protos/bundle.proto",
                "src/protos/protos/relayer.proto",
                "src/protos/protos/shared.proto",
            ],
            &["src/protos/protos"],
        )
        .unwrap();
}

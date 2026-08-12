cargo build --target=aarch64-unknown-none --no-default-features --features rkyv,serde,legacy
cargo build --release --target=aarch64-unknown-none --no-default-features --features rkyv,serde,legacy
cargo clippy --no-default-features
cargo clippy --no-default-features --features rkyv
cargo clippy --no-default-features --features serde
cargo clippy --no-default-features --features legacy
cargo clippy --no-default-features --features rkyv,serde
cargo clippy --no-default-features --features rkyv,legacy
cargo clippy --no-default-features --features serde,legacy
cargo clippy --no-default-features --features rkyv,serde,legacy
cargo clippy --release --no-default-features
cargo clippy --release --no-default-features --features rkyv
cargo clippy --release --no-default-features --features serde
cargo clippy --release --no-default-features --features legacy
cargo clippy --release --no-default-features --features rkyv,serde
cargo clippy --release --no-default-features --features rkyv,legacy
cargo clippy --release --no-default-features --features serde,legacy
cargo clippy --release --no-default-features --features rkyv,serde,legacy

# Loro integration will soon be separated into new crate
cargo build --no-default-features --features loro,rkyv,serde,legacy
cargo build --release --no-default-features --features loro,rkyv,serde,legacy
cargo clippy --no-default-features --features loro
cargo clippy --no-default-features --features loro,rkyv
cargo clippy --no-default-features --features loro,serde
cargo clippy --no-default-features --features loro,legacy
cargo clippy --no-default-features --features loro,rkyv,serde
cargo clippy --no-default-features --features loro,rkyv,legacy
cargo clippy --no-default-features --features loro,serde,legacy
cargo clippy --no-default-features --features loro,rkyv,serde,legacy
cargo clippy --release --no-default-features --features loro
cargo clippy --release --no-default-features --features loro,rkyv
cargo clippy --release --no-default-features --features loro,serde
cargo clippy --release --no-default-features --features loro,legacy
cargo clippy --release --no-default-features --features loro,rkyv,serde
cargo clippy --release --no-default-features --features loro,rkyv,legacy
cargo clippy --release --no-default-features --features loro,serde,legacy
cargo clippy --release --no-default-features --features loro,rkyv,serde,legacy